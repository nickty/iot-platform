/**
 * Build an S3 key using the Bronze zone layout.
 *
 * Layout: {zone}/year=YYYY/month=MM/day=DD/hour=HH/{prefix}-{ts}-{seq}.parquet
 *
 * Why this layout:
 * - Time-based partition pruning: queries filter by date → only scan relevant hours
 * - Hive-style partitions (key=value) are recognized by Athena, Spark, DuckDB
 * - Hourly granularity balances "too many files" vs. "file too big"
 */
export function buildS3Key(
  zone: string,
  timestamp: Date,
  filename: string,
): string {
  const yyyy = timestamp.getUTCFullYear();
  const mm = String(timestamp.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(timestamp.getUTCDate()).padStart(2, '0');
  const hh = String(timestamp.getUTCHours()).padStart(2, '0');

  return `${zone}/year=${yyyy}/month=${mm}/day=${dd}/hour=${hh}/${filename}`;
}

/**
 * Generate a unique filename for a Parquet batch.
 * Format: batch-{timestamp}-{sequence}.parquet
 */
export function buildFilename(sequence: number): string {
  const ts = Date.now();
  const seq = String(sequence).padStart(6, '0');
  return `batch-${ts}-${seq}.parquet`;
}