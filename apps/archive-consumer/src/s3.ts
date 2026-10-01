import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';

let client: S3Client | undefined;

export function getS3(): S3Client {
  if (client) return client;

  const config = getConfig();
  const logger = getLogger();

  client = new S3Client({
    endpoint: config.S3_ENDPOINT,
    region: config.S3_REGION,
    credentials: {
      accessKeyId: config.S3_ACCESS_KEY,
      secretAccessKey: config.S3_SECRET_KEY,
    },
    forcePathStyle: config.S3_FORCE_PATH_STYLE,   // LocalStack requires this
  });

  logger.info({ endpoint: config.S3_ENDPOINT }, 'S3 client initialized');
  return client;
}

export async function uploadParquet(
  bucket: string,
  key: string,
  body: Buffer,
): Promise<void> {
  const s3 = getS3();
  const logger = getLogger();

  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: 'application/vnd.apache.parquet',
    }),
  );

  logger.info({ bucket, key, bytes: body.length }, 'Parquet file uploaded');
}