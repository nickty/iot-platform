import { S3Client, GetObjectCommand, PutObjectCommand, NoSuchKey } from '@aws-sdk/client-s3';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';

const MANIFEST_KEY = '_manifests/silver-processed.json';

export interface Manifest {
  processedKeys: string[];
  lastUpdated: string;
  version: number;
}

async function loadManifest(s3: S3Client, bucket: string): Promise<Manifest> {
  try {
    const res = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: MANIFEST_KEY }),
    );
    const body = await res.Body?.transformToString();
    if (!body) throw new Error('Empty manifest');
    return JSON.parse(body) as Manifest;
  } catch (err) {
    if (err instanceof NoSuchKey || (err as { name?: string }).name === 'NoSuchKey') {
      return { processedKeys: [], lastUpdated: new Date().toISOString(), version: 1 };
    }
    throw err;
  }
}

export async function getManifest(
  s3: S3Client,
  bucket: string,
): Promise<Manifest> {
  return loadManifest(s3, bucket);
}

export async function saveManifest(
  s3: S3Client,
  bucket: string,
  manifest: Manifest,
): Promise<void> {
  const logger = getLogger();
  manifest.lastUpdated = new Date().toISOString();

  // Keep only the last 10,000 keys to prevent unbounded growth
  if (manifest.processedKeys.length > 10000) {
    manifest.processedKeys = manifest.processedKeys.slice(-10000);
  }

  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: MANIFEST_KEY,
      Body: JSON.stringify(manifest, null, 2),
      ContentType: 'application/json',
    }),
  );

  logger.info(
    { processedCount: manifest.processedKeys.length },
    'Manifest saved',
  );
}