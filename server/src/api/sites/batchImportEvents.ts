import { FastifyReply, FastifyRequest } from "fastify";
import { z } from "zod";
import { getUserHasAdminAccessToSite } from "../../lib/auth-utils.js";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { updateImportProgress, updateImportStatus, getImportById } from "../../services/import/importStatusManager.js";
import { UmamiImportMapper, type UmamiEvent } from "../../services/import/mappings/umami.js";
import { ImportQuotaTracker } from "../../services/import/importQuotaChecker.js";
import { db } from "../../db/postgres/postgres.js";
import { sites, importStatus } from "../../db/postgres/schema.js";
import { eq } from "drizzle-orm";
import { createServiceLogger } from "../../lib/logger/logger.js";

const logger = createServiceLogger("import:batch");

// Zod schema for Umami event (from CSV)
const umamiEventSchema = z.object({
  session_id: z.string(),
  hostname: z.string(),
  browser: z.string(),
  os: z.string(),
  device: z.string(),
  screen: z.string(),
  language: z.string(),
  country: z.string(),
  region: z.string(),
  city: z.string(),
  url_path: z.string(),
  url_query: z.string(),
  referrer_path: z.string(),
  referrer_query: z.string(),
  referrer_domain: z.string(),
  page_title: z.string(),
  event_type: z.string(),
  event_name: z.string(),
  distinct_id: z.string(),
  created_at: z.string(),
});

const batchImportRequestSchema = z
  .object({
    params: z.object({
      site: z.string().min(1),
    }),
    body: z.object({
      events: z.array(umamiEventSchema).min(1).max(10000), // Properly typed Umami events
      importId: z.string().uuid(),
    }),
  })
  .strict();

type BatchImportRequest = {
  Params: z.infer<typeof batchImportRequestSchema.shape.params>;
  Body: z.infer<typeof batchImportRequestSchema.shape.body>;
};

export async function batchImportEvents(request: FastifyRequest<BatchImportRequest>, reply: FastifyReply) {
  try {
    const parsed = batchImportRequestSchema.safeParse({
      params: request.params,
      body: request.body,
    });

    if (!parsed.success) {
      logger.error({ error: parsed.error }, "Validation error");
      return reply.status(400).send({ error: "Validation error", details: parsed.error.flatten() });
    }

    const { site } = parsed.data.params;
    const { events, importId } = parsed.data.body;
    const siteId = Number(site);

    const userHasAccess = await getUserHasAdminAccessToSite(request, site);
    if (!userHasAccess) {
      return reply.status(403).send({ error: "Forbidden" });
    }

    // Verify import exists and is in valid state
    const importRecord = await getImportById(importId);
    if (!importRecord) {
      logger.error({ importId }, "Import not found");
      return reply.status(404).send({ error: "Import not found" });
    }

    if (importRecord.siteId !== siteId) {
      logger.error({ importId, siteId, recordSiteId: importRecord.siteId }, "Import site mismatch");
      return reply.status(400).send({ error: "Import does not belong to this site" });
    }

    if (importRecord.status === "completed") {
      logger.warn({ importId }, "Attempt to add events to completed import");
      return reply.status(400).send({ error: "Import already completed" });
    }

    if (importRecord.status === "failed") {
      logger.warn({ importId }, "Attempt to add events to failed import");
      return reply.status(400).send({ error: "Import has failed" });
    }

    // Update status to processing if still pending
    if (importRecord.status === "pending") {
      await updateImportStatus(importId, "processing");
    }

    // Auto-detect platform if not set (first batch)
    let detectedPlatform = importRecord.platform;
    if (!detectedPlatform) {
      // Detect platform based on event structure
      // For now, we only support Umami, so if events match Umami schema, it's Umami
      detectedPlatform = "umami";

      // Update import record with detected platform
      await db
        .update(importStatus)
        .set({ platform: detectedPlatform })
        .where(eq(importStatus.importId, importId));

      logger.info({ importId, detectedPlatform }, "Auto-detected platform");
    }

    // Get organization ID for quota checking
    const [siteRecord] = await db
      .select({ organizationId: sites.organizationId })
      .from(sites)
      .where(eq(sites.siteId, siteId))
      .limit(1);

    if (!siteRecord) {
      logger.error({ siteId }, "Site not found");
      return reply.status(404).send({ error: "Site not found" });
    }

    try {
      // Create quota tracker for server-side quota checking
      const quotaTracker = await ImportQuotaTracker.create(siteRecord.organizationId);

      // Filter events based on quota availability
      const eventsWithinQuota: UmamiEvent[] = [];
      let skippedDueToQuota = 0;

      for (const event of events) {
        if (!event.created_at) {
          continue; // Skip events without timestamp
        }

        if (quotaTracker.canImportEvent(event.created_at)) {
          eventsWithinQuota.push(event);
        } else {
          skippedDueToQuota++;
        }
      }

      // If all events were skipped due to quota, log warning
      if (eventsWithinQuota.length === 0 && events.length > 0) {
        const quotaSummary = quotaTracker.getSummary();
        const errorMessage =
          `All ${events.length} events exceeded monthly quotas or fell outside the ${quotaSummary.totalMonthsInWindow}-month historical window. ` +
          `${quotaSummary.monthsAtCapacity} of ${quotaSummary.totalMonthsInWindow} months are at full capacity.`;

        logger.warn({ importId, skippedDueToQuota }, errorMessage);

        return reply.status(400).send({
          success: false,
          error: "Quota exceeded",
          message: errorMessage,
        });
      }

      // Transform events using server-side mapper (single source of truth)
      const transformedEvents = UmamiImportMapper.transform(eventsWithinQuota, site, importId);

      if (transformedEvents.length === 0) {
        logger.warn({ importId }, "No valid events after transformation");
        return reply.send({
          success: true,
          importedCount: 0,
          message: "No valid events in batch",
        });
      }

      // Insert transformed events into ClickHouse
      await clickhouse.insert({
        table: "events",
        values: transformedEvents,
        format: "JSONEachRow",
      });

      logger.info(
        {
          importId,
          eventCount: transformedEvents.length,
          skippedDueToQuota,
        },
        "Batch inserted successfully"
      );

      // Update progress
      await updateImportProgress(importId, transformedEvents.length);

      return reply.send({
        success: true,
        importedCount: transformedEvents.length,
        message: `Imported ${transformedEvents.length} events${skippedDueToQuota > 0 ? ` (${skippedDueToQuota} skipped due to quota)` : ""}`,
      });
    } catch (insertError) {
      logger.error({ importId, error: insertError }, "Failed to insert batch");

      return reply.status(500).send({
        success: false,
        error: "Failed to insert events",
        message: insertError instanceof Error ? insertError.message : "Unknown error",
      });
    }
  } catch (error) {
    logger.error({ error }, "Unexpected error in batch import");
    return reply.status(500).send({ error: "Internal server error" });
  }
}
