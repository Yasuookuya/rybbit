import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Readable } from "stream";
import { gunzipSync } from "zlib";
import { compress as zstdCompress, decompress as zstdDecompress } from "@mongodb-js/zstd";
import { IS_CLOUD } from "../../lib/const.js";
import { createServiceLogger } from "../../lib/logger/logger.js";

class R2StorageService {
  private readonly client: S3Client | null = null;
  private readonly bucketName: string = "";
  private readonly enabled: boolean = false;
  private logger = createServiceLogger("r2-storage");
  private readonly IMPORT_FILES_PREFIX = "imports/";

  constructor() {
    // Only initialize R2 in cloud environment
    if (IS_CLOUD && process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY) {
      // Create a custom HTTP handler that strips checksum headers
      const httpHandler = new NodeHttpHandler();
      const originalHandle = httpHandler.handle.bind(httpHandler);

      httpHandler.handle = async (request: any, options?: any) => {
        const response = await originalHandle(request, options);

        // Wrap the response to strip checksum headers
        return {
          ...response,
          response: {
            ...response.response,
            headers: Object.fromEntries(
              Object.entries(response.response.headers).filter(([key]) => !key.toLowerCase().includes("checksum"))
            ),
          },
        };
      };

      this.client = new S3Client({
        region: "auto",
        endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
          accessKeyId: process.env.R2_ACCESS_KEY_ID,
          secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
        },
        // Required for R2 compatibility
        forcePathStyle: true,
        // Use our custom HTTP handler
        requestHandler: httpHandler,
      });

      this.bucketName = process.env.R2_BUCKET_NAME || "rybbit";
      this.enabled = true;
      this.logger.info({ bucket: this.bucketName }, "R2Storage initialized");
    } else {
      this.logger.debug("R2Storage not enabled - missing IS_CLOUD or R2 credentials");
    }
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  /**
   * Store a batch of event data in R2
   * Returns the storage key if successful, null if R2 is disabled
   */
  async storeBatch(siteId: number, sessionId: string, eventDataArray: any[]): Promise<string | null> {
    if (!this.enabled || !this.client) {
      return null;
    }

    const timestamp = Date.now();
    const key = `${siteId}/${sessionId}/${timestamp}.json.zst`;

    try {
      // Compress with zstd - much faster decompression than brotli
      const jsonBuffer = Buffer.from(JSON.stringify(eventDataArray));
      const compressed = await zstdCompress(jsonBuffer, 3); // level 3 = good balance

      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucketName,
          Key: key,
          Body: compressed,
          ContentType: "application/octet-stream", // Binary data, not JSON
          // Don't set ContentEncoding - that triggers automatic decompression
          Metadata: {
            siteId: siteId.toString(),
            sessionId: sessionId,
            eventCount: eventDataArray.length.toString(),
            compression: "zstd",
          },
        })
      );

      return key;
    } catch (error) {
      console.error("[R2Storage] Failed to store batch:", error);
      throw error;
    }
  }

  /**
   * Retrieve a batch of event data from R2
   * Returns the decompressed event data array
   */
  async getBatch(key: string): Promise<any[]> {
    if (!this.enabled || !this.client) {
      throw new Error("R2 storage is not enabled");
    }

    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucketName,
          Key: key,
        })
      );

      if (!response.Body) {
        throw new Error("Empty response body");
      }

      // Convert stream to buffer
      const chunks: Uint8Array[] = [];
      const stream = response.Body as Readable;

      for await (const chunk of stream) {
        chunks.push(chunk);
      }

      const buffer = Buffer.concat(chunks);

      // Check if data is already JSON (uncompressed)
      // This happens when ContentEncoding was set to "zstd" and R2 auto-decompressed it
      const bufferStr = buffer.toString("utf8", 0, Math.min(buffer.length, 100));
      const isLikelyJSON = bufferStr.trimStart().startsWith("[") || bufferStr.trimStart().startsWith("{");

      if (isLikelyJSON) {
        try {
          // Try parsing as JSON first
          return JSON.parse(buffer.toString());
        } catch (e) {
          // Not valid JSON, proceed with decompression
        }
      }

      // Try to decompress based on file extension
      let decompressed: Buffer;

      try {
        if (key.endsWith(".zst")) {
          decompressed = await zstdDecompress(buffer);
        } else if (key.endsWith(".gz")) {
          decompressed = gunzipSync(buffer);
        } else {
          // Assume zstd for unknown extensions
          decompressed = await zstdDecompress(buffer);
        }
        return JSON.parse(decompressed.toString());
      } catch (decompressionError: any) {
        // If decompression fails and we haven't tried JSON yet, try it now
        if (!isLikelyJSON) {
          try {
            return JSON.parse(buffer.toString());
          } catch (jsonError) {
            // Data is truly corrupted, throw the original error
            throw decompressionError;
          }
        }
        throw decompressionError;
      }
    } catch (error) {
      console.error("[R2Storage] Failed to retrieve batch:", error);
      throw error;
    }
  }

  /**
   * Delete a batch from R2 (for cleanup)
   */
  async deleteBatch(key: string): Promise<void> {
    if (!this.enabled || !this.client) {
      return;
    }

    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.bucketName,
          Key: key,
        })
      );
    } catch (error) {
      console.error("[R2Storage] Failed to delete batch:", error);
      // Non-critical error, log but don't throw
    }
  }

  /**
   * Store an import CSV file in R2 using streaming upload
   */
  async storeImportFile(key: string, fileStream: Readable): Promise<void> {
    if (!this.enabled || !this.client) {
      throw new Error("R2 storage is not enabled");
    }

    if (key.trim().length === 0) {
      throw new Error("Invalid storage key: key cannot be empty");
    }

    if (key.includes("..")) {
      throw new Error("Invalid storage key: path traversal not allowed");
    }

    try {
      const upload = new Upload({
        client: this.client,
        params: {
          Bucket: this.bucketName,
          Key: key,
          Body: fileStream,
          ContentType: "text/csv",
        },
        queueSize: 4,
        partSize: 5 * 1024 * 1024,
        leavePartsOnError: false,
      });

      await upload.done();
      this.logger.info({ key }, "Successfully stored import file");
    } catch (error) {
      this.logger.error({ error, key }, "Failed to store import file");
      throw error;
    }
  }

  /**
   * Retrieve an import CSV file from R2 as a stream
   */
  async getImportFileStream(key: string): Promise<Readable> {
    if (!this.enabled || !this.client) {
      throw new Error("R2 storage is not enabled");
    }

    if (key.trim().length === 0) {
      throw new Error("Invalid storage key: key cannot be empty");
    }

    if (key.includes("..")) {
      throw new Error("Invalid storage key: path traversal not allowed");
    }

    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucketName,
          Key: key,
        })
      );

      if (!response.Body) {
        throw new Error("Empty response body");
      }

      this.logger.info({ key }, "Successfully retrieved import file stream");
      return response.Body as Readable;
    } catch (error) {
      this.logger.error({ error, key }, "Failed to retrieve import file stream");
      throw error;
    }
  }

  /**
   * Delete an import file from R2 (for cleanup after processing)
   */
  async deleteImportFile(key: string): Promise<void> {
    if (!this.enabled || !this.client) {
      return;
    }

    if (key.trim().length === 0) {
      throw new Error("Invalid storage key: key cannot be empty");
    }

    if (key.includes("..")) {
      throw new Error("Invalid storage key: path traversal not allowed");
    }

    try {
      await this.client.send(
        new DeleteObjectCommand({
          Bucket: this.bucketName,
          Key: key,
        })
      );
      this.logger.info({ key }, "Successfully deleted import file");
    } catch (error) {
      this.logger.error({ error, key }, "Failed to delete import file");
      // Non-critical error, log but don't throw
    }
  }

  /**
   * List all import files in R2
   * Returns array of objects with key and lastModified date
   */
  async listImportFiles(): Promise<Array<{ key: string; lastModified: Date }>> {
    if (!this.enabled || !this.client) {
      return [];
    }

    try {
      const allFiles: Array<{ key: string; lastModified: Date }> = [];
      let continuationToken: string | undefined;

      do {
        const response = await this.client.send(
          new ListObjectsV2Command({
            Bucket: this.bucketName,
            Prefix: this.IMPORT_FILES_PREFIX,
            ContinuationToken: continuationToken,
          })
        );

        if (response.Contents) {
          for (const obj of response.Contents) {
            if (obj.Key && obj.LastModified) {
              allFiles.push({
                key: obj.Key,
                lastModified: obj.LastModified,
              });
            }
          }
        }

        continuationToken = response.NextContinuationToken;
      } while (continuationToken);

      this.logger.info({ count: allFiles.length }, "Listed import files");
      return allFiles;
    } catch (error) {
      this.logger.error({ error }, "Failed to list import files");
      throw error;
    }
  }

  /**
   * Delete all import files older than the specified number of days
   * Returns count of deleted files
   */
  async deleteOldImportFiles(daysOld: number = 1): Promise<number> {
    if (!this.enabled || !this.client) {
      return 0;
    }

    try {
      const files = await this.listImportFiles();
      const cutoffDate = new Date(Date.now() - daysOld * 24 * 60 * 60 * 1000);

      const filesToDelete = files.filter(file => file.lastModified < cutoffDate);

      this.logger.info(
        { total: files.length, toDelete: filesToDelete.length, cutoffDate },
        "Deleting old import files"
      );

      let deletedCount = 0;
      for (const file of filesToDelete) {
        try {
          await this.deleteImportFile(file.key);
          deletedCount++;
        } catch (error) {
          this.logger.error({ error, key: file.key }, "Failed to delete old import file");
          // Continue with other files even if one fails
        }
      }

      this.logger.info({ deletedCount }, "Completed deleting old import files");
      return deletedCount;
    } catch (error) {
      this.logger.error({ error }, "Failed to delete old import files");
      throw error;
    }
  }
}

// Singleton instance
export const r2Storage = new R2StorageService();
