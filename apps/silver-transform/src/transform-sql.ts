/**
 * Build the SQL to transform Bronze Parquet → Silver Parquet.
 *
 * Transformations applied:
 * 1. Deduplicate by event_id (Kafka delivers at-least-once)
 * 2. Filter out events with null device_id or timestamp
 * 3. Add derived columns:
 *    - hour_of_day, day_of_week, date
 *    - temp_category: low|normal|high|critical
 *    - is_high_temp flag
 * 4. Rename columns for clarity
 * 5. Cast types for consistency
 */
export function buildSilverTransformSql(
  bronzeFiles: string[],
  outputFile: string,
): string {
  // Escape single quotes in file paths
  const escapedFiles = bronzeFiles
    .map((f) => `'${f.replace(/'/g, "''")}'`)
    .join(', ');

  return `
    COPY (
      WITH deduped AS (
        SELECT
          event_id,
          device_id,
          timestamp,
          ingested_at,
          source,
          temperature,
          humidity,
          pressure,
          region,
          model,
          -- Deduplicate: keep the latest ingestion per event_id
          ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC
          ) AS rn
        FROM read_parquet([${escapedFiles}])
        WHERE
          event_id IS NOT NULL
          AND device_id IS NOT NULL
          AND timestamp IS NOT NULL
      )
      SELECT
        event_id,
        device_id,
        timestamp,
        ingested_at,
        -- Latency: how long between device event and server ingestion
        date_diff('millisecond', timestamp, ingested_at) AS ingest_latency_ms,
        source,
        temperature,
        humidity,
        pressure,
        region,
        model,
        -- Time-based derived fields
        EXTRACT(HOUR FROM timestamp) AS hour_of_day,
        EXTRACT(DOW FROM timestamp) AS day_of_week,
        CAST(timestamp AS DATE) AS event_date,
        -- Business logic: categorize temperature
        CASE
          WHEN temperature IS NULL THEN 'unknown'
          WHEN temperature < 15 THEN 'low'
          WHEN temperature < 30 THEN 'normal'
          WHEN temperature < 40 THEN 'high'
          ELSE 'critical'
        END AS temp_category,
        temperature >= 30 AS is_high_temp,
        humidity < 20 AS is_low_humidity,
        pressure < 1000 AS is_low_pressure
      FROM deduped
      WHERE rn = 1
    )
    TO '${outputFile.replace(/'/g, "''")}'
    (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000);
  `;
}

/**
 * SQL to count rows in a Parquet file (for verification).
 */
export function buildCountSql(file: string): string {
  return `SELECT COUNT(*) AS count FROM read_parquet('${file.replace(/'/g, "''")}')`;
}

/**
 * SQL to inspect schema of a Parquet file.
 */
export function buildSchemaSql(file: string): string {
  return `DESCRIBE SELECT * FROM read_parquet('${file.replace(/'/g, "''")}')`;
}