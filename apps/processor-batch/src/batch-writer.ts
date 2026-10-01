import type { Sql } from 'postgres';
import type { TelemetryEvent } from '@iot/contracts';
import { getLogger } from '@iot/observability';

export interface BatchWriterOptions {
  maxBatchSize: number;        // flush when buffer reaches this size
  maxBufferMs: number;         // flush at least this often
}

interface PendingEvent {
  time: Date;
  deviceId: string;
  eventId: string;
  metrics: Record<string, number>;
  tags: Record<string, string>;
  source: string;
  ingestedAt: Date;
}

export class BatchWriter {
  private buffer: PendingEvent[] = [];
  private flushTimer: NodeJS.Timeout | undefined;
  private flushing = false;
  private closed = false;

  constructor(
    private sql: Sql,
    private options: BatchWriterOptions,
  ) {
    this.scheduleFlush();
  }

  async add(event: TelemetryEvent): Promise<void> {
    if (this.closed) {
      throw new Error('BatchWriter is closed');
    }

    this.buffer.push({
      time: new Date(event.timestamp),
      deviceId: event.deviceId,
      eventId: event.eventId,
      metrics: event.metrics,
      tags: event.tags ?? {},
      source: event.source,
      ingestedAt: new Date(event.ingestedAt),
    });

    if (this.buffer.length >= this.options.maxBatchSize) {
      await this.flush();
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

    const logger = getLogger();
    const startTime = performance.now();

    try {
      // Bulk insert using postgres.js's helper
      // The column list must match the object keys
      await this.sql`
        INSERT INTO telemetry ${this.sql(toWrite.map((e) => ({
          time: e.time,
          device_id: e.deviceId,
          event_id: e.eventId,
          metrics: e.metrics,
          tags: e.tags,
          source: e.source,
          ingested_at: e.ingestedAt,
        })))}
      `;

      const durationMs = performance.now() - startTime;
      logger.info(
        { count: toWrite.length, durationMs: durationMs.toFixed(2) },
        'Batch inserted',
      );
    } catch (err) {
      logger.error(
        { err, count: toWrite.length },
        'Batch insert failed — re-queueing events',
      );
      // Re-queue for retry on next flush
      this.buffer = [...toWrite, ...this.buffer];
      // Small backoff before next attempt
      await new Promise((r) => setTimeout(r, 1000));
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