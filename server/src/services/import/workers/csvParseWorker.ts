import { access, constants } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { parse } from "@fast-csv/parse";
import { DateTime } from "luxon";
import { IJobQueue } from "../queues/jobQueue.js";
import { r2Storage } from "../../storage/r2StorageService.js";
import { CSV_PARSE_QUEUE, CsvParseJob, DATA_INSERT_QUEUE, DataInsertJob } from "./jobs.js";
import { UmamiEvent, umamiHeaders } from "../mappings/umami.js";
import { updateImportStatus } from "../importStatusManager.js";
import { deleteImportFile } from "../utils.js";
import { ImportQuotaTracker } from "../importQuotaChecker.js";
import { createServiceLogger } from "../../../lib/logger/logger.js";

const logger = createServiceLogger("import:csv-parser");

const getImportDataHeaders = (platform: string) => {
  switch (platform) {
    case "umami":
      return umamiHeaders;
    default:
      throw new Error(`Unsupported platform: ${platform}`);
  }
};

const safeUpdateStatusToFailed = async (importId: string, errorMessage: string) => {
  try {
    await updateImportStatus(importId, "failed", errorMessage);
  } catch (updateError) {
    logger.error({ importId, error: updateError }, "Could not update status to failed");
  }
};

const createR2FileStream = async (storageLocation: string, platform: string) => {
  logger.info({ storageLocation }, "Reading from R2");
  const fileStream = await r2Storage.getImportFileStream(storageLocation);
  return fileStream.pipe(
    parse({
      headers: getImportDataHeaders(platform),
      renameHeaders: true,
      ignoreEmpty: true,
    })
  );
};

const createLocalFileStream = async (storageLocation: string, platform: string) => {
  logger.info({ storageLocation }, "Reading from local disk");
  await access(storageLocation, constants.F_OK | constants.R_OK);
  return createReadStream(storageLocation).pipe(
    parse({
      headers: getImportDataHeaders(platform),
      renameHeaders: true,
      ignoreEmpty: true,
    })
  );
};

const createDateRangeFilter = (startDateStr?: string, endDateStr?: string) => {
  const startDate = startDateStr
    ? DateTime.fromFormat(startDateStr, "yyyy-MM-dd", { zone: "utc" }).startOf("day")
    : null;
  const endDate = endDateStr ? DateTime.fromFormat(endDateStr, "yyyy-MM-dd", { zone: "utc" }).endOf("day") : null;

  if (startDate && !startDate.isValid) {
    throw new Error(`Invalid start date: ${startDateStr}`);
  }

  if (endDate && !endDate.isValid) {
    throw new Error(`Invalid end date: ${endDateStr}`);
  }

  return (dateStr: string) => {
    const createdAt = DateTime.fromFormat(dateStr, "yyyy-MM-dd HH:mm:ss", { zone: "utc" });
    if (!createdAt.isValid) {
      return false;
    }

    if (startDate && createdAt < startDate) {
      return false;
    }

    if (endDate && createdAt > endDate) {
      return false;
    }

    return true;
  };
};

export async function createCsvParseWorker(jobQueue: IJobQueue) {
  await jobQueue.work<CsvParseJob>(CSV_PARSE_QUEUE, async job => {
    const { site, importId, platform, storageLocation, isR2Storage, organization, startDate, endDate } = job;

    let stream: ReturnType<typeof parse> | null = null;
    let processingTimeout: NodeJS.Timeout | null = null;

    try {
      const quotaTracker = await ImportQuotaTracker.create(organization);

      const chunkSize = 5000;
      const INACTIVITY_TIMEOUT_MS = 90 * 1000;

      let chunk: UmamiEvent[] = [];
      let totalSkippedQuota = 0;
      let totalProcessed = 0;

      stream = isR2Storage
        ? await createR2FileStream(storageLocation, platform)
        : await createLocalFileStream(storageLocation, platform);

      stream.on("error", error => {
        logger.error({ importId, error }, "Stream error");
      });

      await updateImportStatus(importId, "processing");
      logger.info({ importId, platform, organization }, "Started processing CSV import");

      const resetTimeout = () => {
        if (processingTimeout) {
          clearTimeout(processingTimeout);
        }
        processingTimeout = setTimeout(() => {
          if (stream) {
            stream.destroy(new Error("Import processing inactivity timeout exceeded"));
          }
        }, INACTIVITY_TIMEOUT_MS);
      };

      // Set initial timeout
      resetTimeout();

      const isDateInRange = createDateRangeFilter(startDate, endDate);

      for await (const data of stream) {
        // Skip rows with missing or invalid dates
        if (!data.created_at) {
          continue;
        }

        // Apply user-specified date range filter
        if (!isDateInRange(data.created_at)) {
          continue;
        }

        // Check per-month quota (includes historical window check)
        if (!quotaTracker.canImportEvent(data.created_at)) {
          totalSkippedQuota++;
          continue;
        }

        chunk.push(data);
        totalProcessed++;

        if (chunk.length >= chunkSize) {
          await jobQueue.send<DataInsertJob>(DATA_INSERT_QUEUE, {
            site,
            importId,
            platform,
            chunk,
            allChunksSent: false,
          });
          chunk = [];
          resetTimeout();
        }
      }

      if (chunk.length > 0) {
        await jobQueue.send<DataInsertJob>(DATA_INSERT_QUEUE, {
          site,
          importId,
          platform,
          chunk,
          allChunksSent: false,
        });
        resetTimeout();
      }

      if (totalSkippedQuota > 0) {
        const quotaSummary = quotaTracker.getSummary();
        const errorMessage =
          `${totalSkippedQuota} events exceeded monthly quotas or fell outside the ${quotaSummary.totalMonthsInWindow}-month historical window. ` +
          `${quotaSummary.monthsAtCapacity} of ${quotaSummary.totalMonthsInWindow} months are at full capacity. ` +
          `Try importing newer data or upgrade your plan for higher monthly quotas.`;
        await updateImportStatus(importId, "failed", errorMessage);
        await deleteImportFile(storageLocation, isR2Storage);
        return;
      }

      logger.info({ importId, totalProcessed, totalSkippedQuota }, "CSV parsing completed successfully");
      await jobQueue.send<DataInsertJob>(DATA_INSERT_QUEUE, {
        site,
        importId,
        platform,
        chunk: [],
        allChunksSent: true,
      });
    } catch (error) {
      logger.error({ importId, error }, "Error in CSV parse worker");
      await safeUpdateStatusToFailed(importId, "An unexpected error occurred during import processing");
      logger.error({ importId }, "Import failed, worker continuing");
    } finally {
      if (processingTimeout) {
        clearTimeout(processingTimeout);
      }

      if (stream) {
        try {
          stream.destroy();
        } catch (streamError) {
          logger.warn({ importId, error: streamError }, "Failed to destroy stream");
        }
      }

      try {
        await deleteImportFile(storageLocation, isR2Storage);
      } catch (deleteError) {
        logger.warn({ importId, error: deleteError }, "File cleanup failed, will remain in storage");
        // File will be orphaned but import status is already recorded
      }
    }
  });
}
