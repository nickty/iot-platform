import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { createWriteStream } from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { join, dirname } from 'node:path';
import { getConfig } from '@iot/config';
import { getLogger, silverFilesProcessedTotal, silverEventsInTotal, silverEventsOutTotal, silverTransformDuration, silverDuplicatesRemovedTotal } from '@iot/observability';
import { runDuckDb, runDuckDbNoResult } from './duckdb-runner.js';
import { buildSilverTransformSql, buildCountSql } from './transform-sql.js';
import { getManifest, saveManifest } from './manifest.js';
import { uploadFileToS3 } from './s3-utils.js';

export interface TransformResult {
  processedFiles: number;
  skippedFiles: number;
  eventsIn: number;
  eventsOut: number;
  duplicatesRemoved: number;
  durationMs: number;
}

export async function runSilverTransform(
  s3: S3Client,
): Promise<TransformResult> {
  const config = getConfig();
  const logger = getLogger();
  const startTime = performance.now();

  const result: TransformResult = {
    processedFiles: 0,
    skippedFiles: 0,
    eventsIn: 0,
    eventsOut: 0,
    duplicatesRemoved: 0,
    durationMs: 0,
  };

  // ── Step 1: Load manifest ───────────────────────────────────────
  const manifest = await getManifest(s3, config.S3_BUCKET_RAW);
  const processedSet = new Set(manifest.processedKeys);
  logger.info({ alreadyProcessed: processedSet.size }, 'Manifest loaded');

  // ── Step 2: List Bronze files ───────────────────────────────────
  const listRes = await s3.send(
    new ListObjectsV2Command({
      Bucket: config.S3_BUCKET_RAW,
      Prefix: 'raw/',
    }),
  );

  const allKeys = (listRes.Contents ?? [])
    .map((obj) => obj.Key)
    .filter((k): k is string => !!k && k.endsWith('.parquet'));

  // Filter out already-processed files
  const newKeys = allKeys.filter((k) => !processedSet.has(k));
  result.skippedFiles = allKeys.length - newKeys.length;

  if (newKeys.length === 0) {
    logger.info({ totalBronze: allKeys.length }, 'No new Bronze files to process');
    result.durationMs = performance.now() - startTime;
    return result;
  }

  // Process a batch at a time
  const batch = newKeys.slice(0, config.SILVER_BATCH_SIZE);
  logger.info({ batchSize: batch.length, totalNew: newKeys.length }, 'Processing batch');

  // ── Step 3: Download Bronze files to staging ────────────────────
  const stagingDir = config.SILVER_STAGING_DIR;
  await rm(stagingDir, { recursive: true, force: true });
  await mkdir(stagingDir, { recursive: true });

  const localBronzeFiles: string[] = [];
  for (const key of batch) {
    const localPath = join(stagingDir, 'bronze', key.replace(/\//g, '_'));
    await mkdir(dirname(localPath), { recursive: true });

    const obj = await s3.send(
      new GetObjectCommand({ Bucket: config.S3_BUCKET_RAW, Key: key }),
    );
    if (!obj.Body) continue;

    await pipeline(obj.Body as Readable, createWriteStream(localPath));
    localBronzeFiles.push(localPath);
  }

  logger.info({ downloaded: localBronzeFiles.length }, 'Bronze files downloaded');

  // ── Step 4: Count events in (before transform) ──────────────────
  const filesList = localBronzeFiles.map((f) => `'${f}'`).join(', ');
  const countInRes = await runDuckDb<{ count: number }>(
    `SELECT COUNT(*) AS count FROM read_parquet([${filesList}])`,
  );
  result.eventsIn = countInRes.rows[0]?.count ?? 0;

  // ── Step 5: Run the transform ───────────────────────────────────
  const outputFile = join(stagingDir, 'silver', `silver-${Date.now()}.parquet`);
  await mkdir(dirname(outputFile), { recursive: true });

  const transformSql = buildSilverTransformSql(localBronzeFiles, outputFile);
  await runDuckDbNoResult(transformSql);

  // ── Step 6: Count events out (after transform) ──────────────────
  const countOutRes = await runDuckDb<{ count: number }>(buildCountSql(outputFile));
  result.eventsOut = countOutRes.rows[0]?.count ?? 0;
  result.duplicatesRemoved = result.eventsIn - result.eventsOut;

  // ── Step 7: Upload Silver to S3 ─────────────────────────────────
  const silverKey = `telemetry/${batch[0]?.replace(/^raw\//, '') ?? 'unknown'}`;
  await uploadFileToS3(
    s3,
    config.S3_BUCKET_CURATED,
    silverKey,
    outputFile,
  );

  // ── Step 8: Update manifest ─────────────────────────────────────
  manifest.processedKeys.push(...batch);
  await saveManifest(s3, config.S3_BUCKET_RAW, manifest);

  // ── Step 9: Metrics ─────────────────────────────────────────────
  result.processedFiles = batch.length;
  result.durationMs = performance.now() - startTime;

  silverFilesProcessedTotal.inc(batch.length);
  silverEventsInTotal.inc(result.eventsIn);
  silverEventsOutTotal.inc(result.eventsOut);
  silverDuplicatesRemovedTotal.inc(result.duplicatesRemoved);
  silverTransformDuration.observe(result.durationMs / 1000);

  logger.info(result, 'Silver transform complete');

  // Cleanup staging
  await rm(stagingDir, { recursive: true, force: true });

  return result;
}