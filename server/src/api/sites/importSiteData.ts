import { createWriteStream } from "node:fs";
import { mkdir } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { pipeline } from "node:stream/promises";
import { FastifyRequest, FastifyReply } from "fastify";
import { DateTime } from "luxon";
import { z } from "zod";
import { getUserHasAdminAccessToSite } from "../../lib/auth-utils.js";
import { getJobQueue } from "../../queues/jobQueueFactory.js";
import { ImportLimiter } from "../../services/import/importLimiter.js";
import { updateImportStatus } from "../../services/import/importStatusManager.js";
import { deleteImportFile, getImportStorageLocation } from "../../services/import/utils.js";
import { CSV_PARSE_QUEUE } from "../../services/import/workers/jobs.js";
import { r2Storage } from "../../services/storage/r2StorageService.js";

const isValidDate = (date: string) => {
  const dt = DateTime.fromFormat(date, "yyyy-MM-dd", { zone: "utc" });
  return dt.isValid;
};

const parseDate = (date: string) => DateTime.fromFormat(date, "yyyy-MM-dd", { zone: "utc" });

const importDataFieldsSchema = z
  .object({
    fields: z
      .object({
        source: z.enum(["umami"]),
        startDate: z.string().refine(isValidDate, { message: "Invalid start date format" }).optional(),
        endDate: z.string().refine(isValidDate, { message: "Invalid end date format" }).optional(),
      })
      .refine(
        data => {
          if (!data.startDate || !data.endDate) return true;
          return parseDate(data.startDate) <= parseDate(data.endDate);
        },
        { message: "Start date must be before or equal to end date" }
      )
      .refine(
        data => {
          if (!data.startDate) return true;
          const today = DateTime.utc().startOf("day");
          return parseDate(data.startDate) <= today;
        },
        { message: "Start date cannot be in the future" }
      ),
  })
  .strict();

const importDataRequestSchema = z
  .object({
    params: z.object({
      site: z.string().min(1),
    }),
  })
  .strict();

type ImportDataRequest = {
  Params: z.infer<typeof importDataRequestSchema.shape.params>;
};

export async function importSiteData(request: FastifyRequest<ImportDataRequest>, reply: FastifyReply) {
  try {
    const parsedParams = importDataRequestSchema.safeParse({
      params: request.params,
    });

    if (!parsedParams.success) {
      return reply.status(400).send({ error: "Validation error" });
    }

    const { site } = parsedParams.data.params;

    const userHasAccess = await getUserHasAdminAccessToSite(request, site);
    if (!userHasAccess) {
      return reply.status(403).send({ error: "Forbidden" });
    }

    const data = await request.file();
    if (!data) {
      return reply.status(400).send({ error: "No file uploaded." });
    }

    if (data.mimetype !== "text/csv" || !data.filename.endsWith(".csv")) {
      return reply.status(400).send({ error: "Invalid file type. Only .csv files are accepted." });
    }

    const parsedFields = importDataFieldsSchema.safeParse({
      fields: {
        source: (data.fields.source as any)?.value,
        startDate: (data.fields.startDate as any)?.value,
        endDate: (data.fields.endDate as any)?.value,
      },
    });

    if (!parsedFields.success) {
      return reply.status(400).send({ error: "Validation error" });
    }

    const { source, startDate, endDate } = parsedFields.data.fields;
    const siteId = Number(site);
    const importId = randomUUID();

    // Check organization and get initial limit check
    const concurrentImportLimitResult = await ImportLimiter.checkConcurrentImportLimit(siteId);
    if (!concurrentImportLimitResult.allowed) {
      return reply.status(429).send({ error: concurrentImportLimitResult.reason });
    }

    const organization = concurrentImportLimitResult.organizationId;

    // Atomically create import status with concurrency check to prevent race conditions
    const createResult = await ImportLimiter.createImportWithConcurrencyCheck({
      importId,
      siteId,
      organizationId: organization,
      source,
      status: "pending",
      fileName: data.filename,
    });

    if (!createResult.success) {
      return reply.status(429).send({ error: createResult.reason });
    }

    const storage = getImportStorageLocation(importId, data.filename);

    try {
      if (storage.isR2) {
        await r2Storage.storeImportFile(storage.location, data.file);
        console.log(`[Import] File streamed to R2: ${storage.location}`);
      } else {
        const importDir = "/tmp/imports";
        await mkdir(importDir, { recursive: true });
        await pipeline(data.file, createWriteStream(storage.location));
        console.log(`[Import] File stored locally: ${storage.location}`);
      }
    } catch (fileError) {
      await updateImportStatus(importId, "failed", "Failed to save uploaded file");
      console.error("Failed to save uploaded file:", fileError);
      return reply.status(500).send({ error: "Could not process file upload." });
    }

    try {
      const jobQueue = getJobQueue();
      await jobQueue.send(CSV_PARSE_QUEUE, {
        site,
        importId,
        source,
        storageLocation: storage.location,
        isR2Storage: storage.isR2,
        organization,
        startDate,
        endDate,
      });
    } catch (queueError) {
      await updateImportStatus(importId, "failed", "Failed to queue import job");
      await deleteImportFile(storage.location, storage.isR2);
      console.error("Failed to enqueue import job:", queueError);
      return reply.status(500).send({ error: "Failed to initiate import process." });
    }

    return reply.status(202).send({
      data: {
        message: "File upload accepted and is now being processed.",
      },
    });
  } catch (error) {
    console.error("Unexpected error during import:", error);
    return reply.status(500).send({ error: "An unexpected error occurred. Please try again later." });
  }
}
