import * as cron from "node-cron";
import { DateTime } from "luxon";
import { db } from "../../db/postgres/postgres.js";
import { importStatus } from "../../db/postgres/schema.js";
import { sql } from "drizzle-orm";
import { deleteImportFile, getImportStorageLocation } from "./utils.js";

class ImportCleanupService {
  private cleanupTask: cron.ScheduledTask | null = null;

  constructor() {}

  /**
   * Initialize the cleanup cron job.
   * Runs daily at 2 AM UTC to clean up orphaned import files.
   */
  initializeCleanupCron() {
    console.info("[ImportCleanup] Initializing cleanup cron");

    // Schedule cleanup to run daily at 2 AM UTC
    this.cleanupTask = cron.schedule(
      "0 2 * * *",
      async () => {
        try {
          await this.cleanupOrphanedFiles();
        } catch (error) {
          console.error("[ImportCleanup] Error during cleanup:", error);
        }
      },
      { timezone: "UTC" }
    );

    console.info("[ImportCleanup] Cleanup initialized (runs daily at 2 AM UTC)");
  }

  /**
   * Clean up orphaned import files that are more than 7 days old
   * and belong to completed or failed imports.
   */
  private async cleanupOrphanedFiles() {
    console.info("[ImportCleanup] Starting cleanup of orphaned import files");

    const sevenDaysAgo = DateTime.utc().minus({ days: 7 }).toISO();

    try {
      // Find completed/failed imports older than 7 days that still have files
      const oldImports = await db
        .select({
          importId: importStatus.importId,
          fileName: importStatus.fileName,
        })
        .from(importStatus)
        .where(
          sql`${importStatus.status} IN ('completed', 'failed')
              AND ${importStatus.startedAt} < ${sevenDaysAgo}`
        );

      console.info(`[ImportCleanup] Found ${oldImports.length} old imports to check`);

      let deletedCount = 0;
      let failedCount = 0;

      for (const importRecord of oldImports) {
        const storage = getImportStorageLocation(importRecord.importId, importRecord.fileName);

        // Attempt to delete the file
        const result = await deleteImportFile(storage.location, storage.isR2);

        if (result.success) {
          deletedCount++;
        } else {
          // File might already be deleted or doesn't exist - this is not an error
          if (result.error?.includes("ENOENT") || result.error?.includes("NoSuchKey")) {
            // File doesn't exist, which is fine
            continue;
          }
          failedCount++;
          console.warn(`[ImportCleanup] Failed to delete file for import ${importRecord.importId}: ${result.error}`);
        }
      }

      console.info(
        `[ImportCleanup] Cleanup complete: ${deletedCount} files deleted, ${failedCount} failures, ${oldImports.length - deletedCount - failedCount} already gone`
      );
    } catch (error) {
      console.error("[ImportCleanup] Error during cleanup:", error);
    }
  }

  async triggerManualCleanup() {
    console.info("[ImportCleanup] Manual cleanup triggered");
    await this.cleanupOrphanedFiles();
  }

  stopCleanupCron() {
    if (this.cleanupTask) {
      this.cleanupTask.stop();
      console.info("[ImportCleanup] Cleanup cron stopped");
    }
  }
}

// Export singleton instance
export const importCleanupService = new ImportCleanupService();
