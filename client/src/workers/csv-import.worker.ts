// Web Worker for client-side CSV parsing only
// This worker runs in a separate thread to avoid blocking the main UI thread
// Transformation happens on the server

import Papa from "papaparse";
import { DateTime } from "luxon";
import type { WorkerMessageToWorker, WorkerMessageToMain, UmamiEvent } from "@/lib/import/types";

const CHUNK_SIZE = 5000; // Number of rows per batch sent to main thread
const PROGRESS_UPDATE_INTERVAL = 1000; // Update progress every 1000 rows

let currentBatch: UmamiEvent[] = [];
let chunkIndex = 0;
let totalParsed = 0;
let totalSkipped = 0;
let totalErrors = 0;
let errorDetails: Array<{ row: number; message: string }> = [];
let lastProgressUpdate = 0;

// Date range filters (client-side optimization to reduce server load)
// Quota-enforced dates (required based on subscription tier)
let earliestAllowedDate: DateTime | null = null;
let latestAllowedDate: DateTime | null = null;
// User-specified optional date filters
let userStartDate: DateTime | null = null;
let userEndDate: DateTime | null = null;

// Umami CSV header mapping
const umamiHeaders = [
  undefined,
  "session_id",
  undefined,
  undefined,
  "hostname",
  "browser",
  "os",
  "device",
  "screen",
  "language",
  "country",
  "region",
  "city",
  "url_path",
  "url_query",
  undefined,
  undefined,
  undefined,
  undefined,
  undefined,
  "referrer_path",
  "referrer_query",
  "referrer_domain",
  "page_title",
  undefined,
  undefined,
  undefined,
  undefined,
  undefined,
  undefined,
  "event_type",
  "event_name",
  undefined,
  "distinct_id",
  "created_at",
  undefined,
];

function createDateRangeFilter(
  earliestAllowedDateStr: string,
  latestAllowedDateStr: string,
  userStartDateStr?: string,
  userEndDateStr?: string
) {
  // Parse quota-enforced dates (required)
  earliestAllowedDate = DateTime.fromFormat(earliestAllowedDateStr, "yyyy-MM-dd", { zone: "utc" }).startOf("day");
  latestAllowedDate = DateTime.fromFormat(latestAllowedDateStr, "yyyy-MM-dd", { zone: "utc" }).endOf("day");

  if (!earliestAllowedDate.isValid) {
    throw new Error(`Invalid earliest allowed date: ${earliestAllowedDateStr}`);
  }

  if (!latestAllowedDate.isValid) {
    throw new Error(`Invalid latest allowed date: ${latestAllowedDateStr}`);
  }

  // Parse user-specified optional dates
  userStartDate = userStartDateStr
    ? DateTime.fromFormat(userStartDateStr, "yyyy-MM-dd", { zone: "utc" }).startOf("day")
    : null;
  userEndDate = userEndDateStr
    ? DateTime.fromFormat(userEndDateStr, "yyyy-MM-dd", { zone: "utc" }).endOf("day")
    : null;

  if (userStartDate && !userStartDate.isValid) {
    throw new Error(`Invalid user start date: ${userStartDateStr}`);
  }

  if (userEndDate && !userEndDate.isValid) {
    throw new Error(`Invalid user end date: ${userEndDateStr}`);
  }
}

function isDateInRange(dateStr: string): boolean {
  const createdAt = DateTime.fromFormat(dateStr, "yyyy-MM-dd HH:mm:ss", { zone: "utc" });
  if (!createdAt.isValid) {
    return false;
  }

  // Check quota-enforced dates (required)
  if (earliestAllowedDate && createdAt < earliestAllowedDate) {
    return false;
  }

  if (latestAllowedDate && createdAt > latestAllowedDate) {
    return false;
  }

  // Check user-specified optional dates
  if (userStartDate && createdAt < userStartDate) {
    return false;
  }

  if (userEndDate && createdAt > userEndDate) {
    return false;
  }

  return true;
}

function sendChunk() {
  if (currentBatch.length > 0) {
    const message: WorkerMessageToMain = {
      type: "CHUNK_READY",
      events: currentBatch,
      chunkIndex,
    };
    self.postMessage(message);
    chunkIndex++;
    currentBatch = [];
  }
}

function sendProgress() {
  const message: WorkerMessageToMain = {
    type: "PROGRESS",
    parsed: totalParsed,
    skipped: totalSkipped,
    errors: totalErrors,
  };
  self.postMessage(message);
}

function handleParsedRow(row: unknown, rowIndex: number) {
  const umamiEvent = row as UmamiEvent;

  // Skip rows with missing created_at (required field)
  if (!umamiEvent.created_at) {
    totalSkipped++;
    return;
  }

  // Apply date range filter (quota-enforced + optional user filters)
  // This client-side filtering reduces network traffic and server processing
  if (!isDateInRange(umamiEvent.created_at)) {
    totalSkipped++;
    return;
  }

  // Add to batch (no transformation, send raw row to server)
  currentBatch.push(umamiEvent);
  totalParsed++;

  // Send batch when it reaches chunk size
  if (currentBatch.length >= CHUNK_SIZE) {
    sendChunk();
  }

  // Send progress update periodically
  if (totalParsed - lastProgressUpdate >= PROGRESS_UPDATE_INTERVAL) {
    sendProgress();
    lastProgressUpdate = totalParsed;
  }
}

function parseCSV(file: File) {
  let rowIndex = 0;

  Papa.parse<UmamiEvent>(file, {
    header: true,
    skipEmptyLines: "greedy", // Skip all empty lines (improved)
    delimiter: "", // Auto-detect delimiter (comma, tab, semicolon, etc.)
    transformHeader: (header, index) => {
      // Map Umami CSV column positions to field names
      return umamiHeaders[index] || header;
    },
    step: results => {
      if (results.data) {
        handleParsedRow(results.data, rowIndex);
        rowIndex++;
      }
      if (results.errors && results.errors.length > 0) {
        totalErrors++;
        if (errorDetails.length < 100) {
          // Limit error collection to avoid memory issues
          errorDetails.push({
            row: rowIndex,
            message: results.errors.map(e => e.message).join(", "),
          });
        }
      }
    },
    complete: () => {
      // Send final chunk if any
      sendChunk();

      // Send final progress
      sendProgress();

      // Send completion message
      const message: WorkerMessageToMain = {
        type: "COMPLETE",
        totalParsed,
        totalSkipped,
        totalErrors,
        errorDetails,
      };
      self.postMessage(message);
    },
    error: error => {
      const message: WorkerMessageToMain = {
        type: "ERROR",
        message: error.message,
        error,
      };
      self.postMessage(message);
    },
  });
}

// Listen for messages from main thread
self.onmessage = (event: MessageEvent<WorkerMessageToWorker>) => {
  const message = event.data;

  switch (message.type) {
    case "PARSE_START":
      // Reset state
      currentBatch = [];
      chunkIndex = 0;
      totalParsed = 0;
      totalSkipped = 0;
      totalErrors = 0;
      errorDetails = [];
      lastProgressUpdate = 0;

      // Set up date range filters (quota-enforced + optional user filters)
      createDateRangeFilter(
        message.earliestAllowedDate,
        message.latestAllowedDate,
        message.startDate,
        message.endDate
      );

      // Start parsing
      parseCSV(message.file);
      break;

    case "CANCEL":
      // Terminate the worker
      self.close();
      break;

    default:
      console.warn("Unknown message type:", message);
  }
};

// Export empty object to make TypeScript happy
export {};
