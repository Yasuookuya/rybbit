import { db } from "../../db/postgres/postgres.js";
import { eq, desc, sql } from "drizzle-orm";
import { importStatus } from "../../db/postgres/schema.js";
import { DateTime } from "luxon";

export type SelectImportStatus = typeof importStatus.$inferSelect;
export type InsertImportStatus = typeof importStatus.$inferInsert;

export async function createImportStatus(data: InsertImportStatus): Promise<void> {
  await db.insert(importStatus).values(data);
}

export async function updateImportStatus(
  importId: string,
  status: InsertImportStatus["status"],
  errorMessage?: string
): Promise<void> {
  const completedAt = status === "completed" || status === "failed" ? DateTime.utc().toISO() : null;

  await db.update(importStatus).set({ status, errorMessage, completedAt }).where(eq(importStatus.importId, importId));
}

export async function updateImportProgress(importId: string, importedEvents: number): Promise<void> {
  await db
    .update(importStatus)
    .set({
      importedEvents: sql`${importStatus.importedEvents} + ${importedEvents}`,
    })
    .where(eq(importStatus.importId, importId));
}

export async function getImportsForSite(siteId: number, limit = 10): Promise<SelectImportStatus[]> {
  return await db.query.importStatus.findMany({
    where: eq(importStatus.siteId, siteId),
    orderBy: [desc(importStatus.startedAt)],
    limit,
  });
}

export async function deleteImport(importId: string): Promise<void> {
  await db.delete(importStatus).where(eq(importStatus.importId, importId));
}

export async function getImportById(importId: string): Promise<SelectImportStatus | undefined> {
  return await db.query.importStatus.findFirst({
    where: eq(importStatus.importId, importId),
  });
}
