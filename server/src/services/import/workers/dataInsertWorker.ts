import { getJobQueue } from "../../../queues/jobQueueFactory.js";
import { UmamiImportMapper } from "../mappings/umami.js";
import { DataInsertJob, DATA_INSERT_QUEUE } from "./jobs.js";
import { clickhouse } from "../../../db/clickhouse/clickhouse.js";
import { updateImportStatus, updateImportProgress } from "../importStatusManager.js";

const getImportDataMapping = (source: string) => {
  switch (source) {
    case "umami":
      return UmamiImportMapper;
    default:
      throw new Error(`Unsupported import source: ${source}`);
  }
};

export async function registerDataInsertWorker() {
  const jobQueue = getJobQueue();

  await jobQueue.work<DataInsertJob>(DATA_INSERT_QUEUE, { batchSize: 1, pollingIntervalSeconds: 2 }, async ([job]) => {
    const { site, importId, source, chunk, chunkNumber, totalChunks, allChunksSent } = job.data;

    // Handle finalization signal
    if (allChunksSent) {
      try {
        await updateImportStatus(importId, "completed");
        console.log(`[Import ${importId}] Completed successfully (${totalChunks ?? 0} chunks processed)`);
        return;
      } catch (error) {
        console.error(`[Import ${importId}] Failed to mark as completed:`, error);
        // Try to update to failed status, but don't crash worker
        try {
          await updateImportStatus(importId, "failed", "Failed to complete import");
        } catch (updateError) {
          console.error(`[Import ${importId}] Could not update status to failed:`, updateError);
        }
        // Don't re-throw - worker should continue
        return;
      }
    }

    // Process data chunk
    try {
      const dataMapper = getImportDataMapping(source);
      const transformedRecords = dataMapper.transform(chunk, site, importId);

      // Insert to ClickHouse (critical - must succeed)
      await clickhouse.insert({
        table: "events",
        values: transformedRecords,
        format: "JSONEachRow",
      });

      // Update progress (non-critical - log if fails but don't crash)
      try {
        await updateImportProgress(importId, transformedRecords.length);
      } catch (progressError) {
        console.warn(
          `[Import ${importId}] Progress update failed (data inserted successfully):`,
          progressError instanceof Error ? progressError.message : progressError
        );
        // Don't throw - data is safely in ClickHouse, progress can be off slightly
      }

      console.log(`[Import ${importId}] Chunk ${chunkNumber ?? "?"} processed: ${transformedRecords.length} events`);
    } catch (error) {
      console.error(`[Import ${importId}] ClickHouse insert failed:`, error);

      // Sanitize error message
      const safeMessage =
        error instanceof Error
          ? `Data insertion failed: ${error.message.substring(0, 500)}`
          : "Data insertion failed due to unknown error";

      try {
        await updateImportStatus(importId, "failed", safeMessage);
      } catch (updateError) {
        console.error(`[Import ${importId}] Could not update status to failed:`, updateError);
      }

      // Don't re-throw - worker should continue processing other jobs
      console.error(`[Import ${importId}] Import chunk failed, worker continuing`);
    }
  });
}
