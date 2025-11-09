// Worker manager for coordinating CSV import process
// Manages worker lifecycle, batch uploads, and error handling
// Server handles transformation and quota checking

import type { WorkerMessageToWorker, WorkerMessageToMain, ImportProgress, FailedBatch, UmamiEvent } from "./types";

const MAX_CONCURRENT_UPLOADS = 3; // Maximum number of parallel batch uploads
const RETRY_ATTEMPTS = 3; // Number of retry attempts for failed batches
const RETRY_DELAY_MS = 1000; // Initial delay between retries (exponential backoff)

type ProgressCallback = (progress: ImportProgress) => void;
type CompleteCallback = (success: boolean, message: string) => void;

export class CSVWorkerManager {
  private worker: Worker | null = null;
  private progress: ImportProgress = {
    status: "idle",
    totalRows: 0,
    parsedRows: 0,
    skippedRows: 0,
    uploadedBatches: 0,
    totalBatches: 0,
    failedBatches: 0,
    importedEvents: 0,
    errors: [],
  };
  private onProgress: ProgressCallback | null = null;
  private onComplete: CompleteCallback | null = null;
  private uploadQueue: Array<{ events: UmamiEvent[]; chunkIndex: number }> = [];
  private activeUploads = 0;
  private completedUploads = new Set<number>();
  private failedBatches: FailedBatch[] = [];
  private parsingComplete = false;
  private siteId: number = 0;
  private importId: string = "";

  constructor(onProgress?: ProgressCallback, onComplete?: CompleteCallback) {
    this.onProgress = onProgress || null;
    this.onComplete = onComplete || null;
  }

  startImport(
    file: File,
    siteId: number,
    importId: string,
    platform: "umami",
    earliestAllowedDate: string,
    latestAllowedDate: string,
    startDate?: string,
    endDate?: string
  ): void {
    this.siteId = siteId;
    this.importId = importId;
    this.parsingComplete = false;
    this.uploadQueue = [];
    this.activeUploads = 0;
    this.completedUploads.clear();
    this.failedBatches = [];

    this.progress = {
      status: "parsing",
      totalRows: 0,
      parsedRows: 0,
      skippedRows: 0,
      uploadedBatches: 0,
      totalBatches: 0,
      failedBatches: 0,
      importedEvents: 0,
      errors: [],
    };

    // Create worker
    this.worker = new Worker(new URL("@/workers/csv-import.worker.ts", import.meta.url), {
      type: "module",
    });

    // Set up message handler
    this.worker.onmessage = (event: MessageEvent<WorkerMessageToMain>) => {
      this.handleWorkerMessage(event.data);
    };

    // Set up error handler
    this.worker.onerror = error => {
      this.progress.status = "failed";
      this.progress.errors.push({
        message: `Worker error: ${error.message}`,
      });
      this.notifyProgress();
      if (this.onComplete) {
        this.onComplete(false, `Worker error: ${error.message}`);
      }
    };

    // Start parsing with allowed date range for client-side filtering
    const message: WorkerMessageToWorker = {
      type: "PARSE_START",
      file,
      siteId,
      importId,
      platform,
      earliestAllowedDate,
      latestAllowedDate,
      startDate,
      endDate,
    };

    this.worker.postMessage(message);
    this.notifyProgress();
  }

  private handleWorkerMessage(message: WorkerMessageToMain): void {
    switch (message.type) {
      case "PROGRESS":
        this.progress.parsedRows = message.parsed;
        this.progress.skippedRows = message.skipped;
        this.progress.errors = this.progress.errors.slice(0, this.progress.errors.length - message.errors);
        for (let i = 0; i < message.errors; i++) {
          this.progress.errors.push({ message: "Parse error" });
        }
        this.notifyProgress();
        break;

      case "CHUNK_READY":
        // Add to upload queue
        this.uploadQueue.push({
          events: message.events,
          chunkIndex: message.chunkIndex,
        });
        this.progress.totalBatches++;
        this.progress.status = "uploading";
        this.notifyProgress();

        // Process upload queue
        this.processUploadQueue();
        break;

      case "COMPLETE":
        this.parsingComplete = true;
        this.progress.parsedRows = message.totalParsed;
        this.progress.skippedRows = message.totalSkipped;

        // Add detailed errors
        message.errorDetails.forEach(error => {
          this.progress.errors.push({
            message: `Row ${error.row}: ${error.message}`,
          });
        });

        this.notifyProgress();

        // Check if all uploads are complete
        this.checkCompletion();
        break;

      case "ERROR":
        this.progress.status = "failed";
        this.progress.errors.push({
          message: message.message,
        });
        this.notifyProgress();
        if (this.onComplete) {
          this.onComplete(false, message.message);
        }
        break;

      default:
        console.warn("Unknown worker message type:", message);
    }
  }

  private async processUploadQueue(): Promise<void> {
    // Process queued uploads with concurrency limit
    while (this.uploadQueue.length > 0 && this.activeUploads < MAX_CONCURRENT_UPLOADS) {
      const batch = this.uploadQueue.shift();
      if (!batch) continue;

      this.activeUploads++;
      this.uploadBatch(batch.events, batch.chunkIndex);
    }
  }

  private async uploadBatch(events: UmamiEvent[], chunkIndex: number, retryCount = 0): Promise<void> {
    try {
      const response = await fetch(`/api/batch-import-events/${this.siteId}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: "include",
        body: JSON.stringify({
          events,
          importId: this.importId,
          batchIndex: chunkIndex,
          totalBatches: this.progress.totalBatches,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ error: "Unknown error" }));
        throw new Error(errorData.error || `HTTP ${response.status}`);
      }

      const data = await response.json();

      // Mark as completed
      this.completedUploads.add(chunkIndex);
      this.progress.uploadedBatches++;
      this.progress.importedEvents += data.importedCount ?? events.length;

      this.activeUploads--;
      this.notifyProgress();

      // Continue processing queue
      this.processUploadQueue();

      // Check if we're done
      this.checkCompletion();
    } catch (error) {
      console.error(`Failed to upload batch ${chunkIndex}:`, error);

      // Retry with exponential backoff
      if (retryCount < RETRY_ATTEMPTS) {
        const delay = RETRY_DELAY_MS * Math.pow(2, retryCount);
        console.log(`Retrying batch ${chunkIndex} in ${delay}ms (attempt ${retryCount + 1}/${RETRY_ATTEMPTS})`);

        setTimeout(() => {
          this.uploadBatch(events, chunkIndex, retryCount + 1);
        }, delay);
      } else {
        // Max retries exceeded, record as failed
        this.failedBatches.push({
          batchIndex: chunkIndex,
          events,
          error: error instanceof Error ? error.message : "Unknown error",
          retryCount,
        });

        this.progress.failedBatches++;
        this.progress.errors.push({
          batch: chunkIndex,
          message: `Batch ${chunkIndex} failed after ${RETRY_ATTEMPTS} attempts: ${error instanceof Error ? error.message : "Unknown error"}`,
        });

        this.activeUploads--;
        this.notifyProgress();

        // Continue processing other batches
        this.processUploadQueue();
        this.checkCompletion();
      }
    }
  }

  private checkCompletion(): void {
    if (this.parsingComplete && this.activeUploads === 0 && this.uploadQueue.length === 0) {
      const totalExpectedBatches = this.progress.totalBatches;
      const successfulBatches = this.progress.uploadedBatches;
      const failedCount = this.progress.failedBatches;

      if (failedCount === 0) {
        this.progress.status = "completed";
        this.notifyProgress();
        if (this.onComplete) {
          this.onComplete(true, `Import completed successfully: ${this.progress.importedEvents} events imported`);
        }
      } else {
        this.progress.status = "completed";
        this.notifyProgress();
        if (this.onComplete) {
          this.onComplete(
            false,
            `Import completed with errors: ${successfulBatches}/${totalExpectedBatches} batches succeeded, ${failedCount} batches failed`
          );
        }
      }

      // Clean up worker
      this.terminate();
    }
  }

  private notifyProgress(): void {
    if (this.onProgress) {
      this.onProgress({ ...this.progress });
    }
  }

  getProgress(): ImportProgress {
    return { ...this.progress };
  }

  getFailedBatches(): FailedBatch[] {
    return [...this.failedBatches];
  }

  terminate(): void {
    if (this.worker) {
      const message: WorkerMessageToWorker = {
        type: "CANCEL",
      };
      this.worker.postMessage(message);
      this.worker.terminate();
      this.worker = null;
    }
  }
}
