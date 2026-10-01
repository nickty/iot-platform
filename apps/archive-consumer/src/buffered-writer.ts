import type { TelemetryEvent } from '@iot/contracts';
import { getLogger, archiveBufferSize, archiveFilesWrittenTotal, archiveBytesWrittenTotal, archiveFlushDuration } from '@iot/observability';
import { getConfig } from '@iot/config';
import { writeParquetBuffer } from './parquet-writer.js';
import { buildS3Key, buildFilename } from './paths.js';
import { uploadParquet } from './s3.js';

export interface BufferedWriterOptions {
  maxBatchSize: number;      // flush when buffer reaches this
  maxBufferMs: number;       // flush at least this often
  zone: string;              // 'raw' for Bronze
}

export class BufferedWriter {
  private buffer: TelemetryEvent[] = [];
  private flushTimer: NodeJS.Timeout | undefined;
  private flushing = false;
  private closed = false;
  private sequence = 0;

  constructor(private options: BufferedWriterOptions) {
    this.scheduleFlush();
  }

  add(event: TelemetryEvent): void {
    if (this.closed) throw new Error('BufferedWriter is closed');
    this.buffer.push(event);
    archiveBufferSize.set(this.buffer.length);

    if (this.buffer.length >= this.options.maxBatchSize) {
      void this.flush();
    }
  }

  private scheduleFlush(): void {
    this.flushTimer = setInterval(() => {
      void this.flush();
    }, this.options.maxBufferMs);
  }

  async flush(): Promise<void> {
    if (this.flushing) return;
    if (this.buffer.length === 0) return;

    this.flushing = true;
    const toWrite = this.buffer;
    this.buffer = [];
    archiveBufferSize.set(0);

    const logger = getLogger();
    const config = getConfig();
    const startTime = performance.now();

    try {
      // Write Parquet
      const parquetBuffer = await writeParquetBuffer(toWrite);

      // Build S3 key
      const now = new Date();
      const filename = buildFilename(++this.sequence);
      const key = buildS3Key(this.options.zone, now, filename);

      // Upload
      await uploadParquet(config.S3_BUCKET_RAW, key, parquetBuffer);

      // Metrics
      const durationSeconds = (performance.now() - startTime) / 1000;
      archiveFlushDuration.observe(durationSeconds);
      archiveFilesWrittenTotal.inc({ zone: this.options.zone });
      archiveBytesWrittenTotal.inc(parquetBuffer.length);

      logger.info(
        {
          count: toWrite.length,
          bytes: parquetBuffer.length,
          durationMs: (durationSeconds * 1000).toFixed(1),
          key,
        },
        'Archive batch flushed',
      );
    } catch (err) {
      logger.error({ err, count: toWrite.length }, 'Archive flush failed — re-queueing');
      this.buffer = [...toWrite, ...this.buffer];
      archiveBufferSize.set(this.buffer.length);
      await new Promise((r) => setTimeout(r, 2000));
    } finally {
      this.flushing = false;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.flushTimer) clearInterval(this.flushTimer);
    await this.flush();
  }
}