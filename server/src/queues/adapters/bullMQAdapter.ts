import { Job, Queue, QueueEvents, Worker } from "bullmq";
import { IJobQueue, JobConfig, JobData, JobResult } from "../jobQueue.js";

export class BullMQAdapter implements IJobQueue {
  private readonly connection: { host: string; port: number; password?: string };
  private queues: Map<string, Queue> = new Map();
  private workers: Map<string, Worker> = new Map();
  private queueEvents: Map<string, QueueEvents> = new Map();

  constructor() {
    this.connection = {
      host: process.env.REDIS_HOST || "localhost",
      port: parseInt(process.env.REDIS_PORT || "6379", 10),
      ...(process.env.REDIS_PASSWORD && { password: process.env.REDIS_PASSWORD }),
    };

    console.info(`[BullMQ] Configuration: Redis at ${this.connection.host}:${this.connection.port}`);
  }

  async start(): Promise<void> {
    console.info("[BullMQ] Started successfully");
  }

  async stop(): Promise<void> {
    console.info("[BullMQ] Stopping...");
    const errors: Error[] = [];

    // Close all workers first
    const workerResults = await Promise.allSettled(Array.from(this.workers.values()).map(worker => worker.close()));
    workerResults.forEach((result, index) => {
      if (result.status === "rejected") {
        const workerName = Array.from(this.workers.keys())[index];
        const error = new Error(`Failed to close worker ${workerName}: ${result.reason}`);
        errors.push(error);
        console.error(`[BullMQ] ${error.message}`);
      }
    });
    this.workers.clear();

    // Close all queue events
    const queueEventsResults = await Promise.allSettled(Array.from(this.queueEvents.values()).map(qe => qe.close()));
    queueEventsResults.forEach((result, index) => {
      if (result.status === "rejected") {
        const queueName = Array.from(this.queueEvents.keys())[index];
        const error = new Error(`Failed to close queue events for ${queueName}: ${result.reason}`);
        errors.push(error);
        console.error(`[BullMQ] ${error.message}`);
      }
    });
    this.queueEvents.clear();

    // Close all queues (this closes their Redis connections)
    const queueResults = await Promise.allSettled(Array.from(this.queues.values()).map(queue => queue.close()));
    queueResults.forEach((result, index) => {
      if (result.status === "rejected") {
        const queueName = Array.from(this.queues.keys())[index];
        const error = new Error(`Failed to close queue ${queueName}: ${result.reason}`);
        errors.push(error);
        console.error(`[BullMQ] ${error.message}`);
      }
    });
    this.queues.clear();

    if (errors.length > 0) {
      const aggregatedError = new Error(
        `BullMQ stop encountered ${errors.length} error(s): ${errors.map(e => e.message).join("; ")}`
      );
      console.error("[BullMQ] Stopped with errors");
      throw aggregatedError;
    }

    console.info("[BullMQ] Stopped successfully");
  }

  async createQueue(queueName: string): Promise<void> {
    if (!this.queues.has(queueName)) {
      let queue: Queue | undefined;
      let queueEvents: QueueEvents | undefined;

      try {
        // Create Queue
        queue = new Queue(queueName, {
          connection: this.connection,
          defaultJobOptions: {
            attempts: 1, // No retries, same as pg-boss
            removeOnComplete: true,
            removeOnFail: false, // Keep failed jobs for debugging
          },
        });

        this.queues.set(queueName, queue);

        // Create QueueEvents for monitoring
        queueEvents = new QueueEvents(queueName, {
          connection: this.connection,
        });

        this.queueEvents.set(queueName, queueEvents);

        // Log job events
        queueEvents.on("completed", ({ jobId }) => {
          console.info(`[BullMQ] Job ${jobId} completed in queue ${queueName}`);
        });

        queueEvents.on("failed", ({ jobId, failedReason }) => {
          console.error(`[BullMQ] Job ${jobId} failed in queue ${queueName}:`, failedReason);
        });
      } catch (error) {
        console.error(`[BullMQ] Failed to create queue ${queueName}:`, error);

        // Clean up any partially created resources
        const cleanupPromises: Promise<void>[] = [];

        if (queueEvents) {
          this.queueEvents.delete(queueName);
          cleanupPromises.push(
            queueEvents.close().catch(closeError => {
              console.error(`[BullMQ] Failed to close queue events during cleanup for ${queueName}:`, closeError);
            })
          );
        }

        if (queue) {
          this.queues.delete(queueName);
          cleanupPromises.push(
            queue.close().catch(closeError => {
              console.error(`[BullMQ] Failed to close queue during cleanup for ${queueName}:`, closeError);
            })
          );
        }

        // Wait for cleanup to complete before rethrowing
        await Promise.all(cleanupPromises);

        throw error;
      }
    }
  }

  async send<T>(queueName: string, data: T, options?: { priority?: number; delay?: number }): Promise<string> {
    const queue = this.queues.get(queueName);
    if (!queue) {
      throw new Error(`Queue ${queueName} not found. Call createQueue first.`);
    }

    try {
      const job = await queue.add(queueName, data, {
        priority: options?.priority,
        delay: options?.delay,
      });

      if (!job.id) {
        throw new Error(`Failed to enqueue job to ${queueName}`);
      }

      return job.id;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`[BullMQ] Failed to enqueue job to ${queueName}:`, error);
      throw new Error(`Failed to enqueue job to ${queueName}: ${errorMessage}`);
    }
  }

  async work<T>(
    queueName: string,
    config: JobConfig,
    handler: (jobs: JobData<T>[]) => Promise<void | JobResult>
  ): Promise<void> {
    try {
      const worker = new Worker(
        queueName,
        async (job: Job<T>) => {
          const normalizedJob: JobData<T> = {
            id: job.id!,
            data: job.data,
          };

          // NOTE: Interface Impedance Mismatch
          // BullMQ processes one job at a time per worker, while pg-boss can batch jobs.
          // The IJobQueue interface expects handlers to accept arrays to support pg-boss batching.
          // Here we wrap single BullMQ jobs in an array to maintain interface compatibility.
          // This is an acceptable tradeoff: pg-boss gets true batching, BullMQ gets array of 1 item.
          await handler([normalizedJob]);
        },
        {
          connection: this.connection,
          concurrency: config.concurrency ?? config.batchSize ?? 1,
          limiter: config.limiter,
        }
      );

      worker.on("error", error => {
        console.error(`[BullMQ] Worker error in queue ${queueName}:`, error);
      });

      worker.on("failed", (job, error) => {
        console.error(`[BullMQ] Job ${job?.id} failed in queue ${queueName}:`, error);
      });

      this.workers.set(queueName, worker);
    } catch (error) {
      console.error(
        `[BullMQ] Failed to create worker for queue ${queueName} with config:`,
        { concurrency: config.concurrency ?? config.batchSize ?? 1, limiter: config.limiter },
        error
      );
      throw error;
    }
  }
}
