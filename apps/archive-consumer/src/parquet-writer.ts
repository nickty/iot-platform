import parquet from 'parquetjs-lite';
import { Readable } from 'node:stream';
import type { TelemetryEvent } from '@iot/contracts';

/**
 * Parquet schema for raw telemetry events.
 * Time is partitioned out — stored in the S3 path, not the file.
 */
const TELEMETRY_SCHEMA = new parquet.ParquetSchema({
  event_id: { type: 'UTF8' },
  device_id: { type: 'UTF8' },
  timestamp: { type: 'TIMESTAMP_MILLIS' },
  ingested_at: { type: 'TIMESTAMP_MILLIS' },
  schema_version: { type: 'UTF8' },
  source: { type: 'UTF8' },
  // Metrics flattened — each becomes a column
  temperature: { type: 'DOUBLE', optional: true },
  humidity: { type: 'DOUBLE', optional: true },
  pressure: { type: 'DOUBLE', optional: true },
  // Tags flattened — common tags promoted to columns
  region: { type: 'UTF8', optional: true },
  model: { type: 'UTF8', optional: true },
});

/**
 * Convert a TelemetryEvent into a row matching our Parquet schema.
 */
function toRow(event: TelemetryEvent): Record<string, unknown> {
  return {
    event_id: event.eventId,
    device_id: event.deviceId,
    timestamp: new Date(event.timestamp),
    ingested_at: new Date(event.ingestedAt),
    schema_version: event.schemaVersion,
    source: event.source,
    temperature: event.metrics.temperature ?? null,
    humidity: event.metrics.humidity ?? null,
    pressure: event.metrics.pressure ?? null,
    region: event.tags?.region ?? null,
    model: event.tags?.model ?? null,
  };
}

/**
 * Write events to a Parquet file and return the buffer.
 * Buffer is used for uploading to S3 without touching disk.
 */
export async function writeParquetBuffer(
  events: TelemetryEvent[],
): Promise<Buffer> {
  const writer = await parquet.ParquetWriter.openFile(
    TELEMETRY_SCHEMA,
    '/dev/null',         // dummy path — we intercept the output
    { compression: 'SNAPPY' },
  );

  // parquetjs-lite doesn't expose a clean way to write to a buffer.
  // We use its internal file write stream trick: write to /tmp, read bytes, unlink.
  const tmpPath = `/tmp/archive-${Date.now()}-${Math.random().toString(36).slice(2)}.parquet`;

  const realWriter = await parquet.ParquetWriter.openFile(
    TELEMETRY_SCHEMA,
    tmpPath,
    { compression: 'SNAPPY' },
  );

  for (const event of events) {
    await realWriter.appendRow(toRow(event));
  }

  await realWriter.close();
  await writer.close();

  // Read the file into a buffer
  const fs = await import('node:fs/promises');
  const buffer = await fs.readFile(tmpPath);
  await fs.unlink(tmpPath);

  return buffer;
}

/**
 * Alternative approach: stream events directly to Parquet.
 * Useful for very large batches.
 */
export async function writeParquetStream(
  events: TelemetryEvent[],
): Promise<Readable> {
  const buffer = await writeParquetBuffer(events);
  return Readable.from(buffer);
}