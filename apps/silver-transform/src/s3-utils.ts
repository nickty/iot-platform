import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { readFile, stat } from 'node:fs/promises';
import { getLogger } from '@iot/observability';

export async function uploadFileToS3(
  s3: S3Client,
  bucket: string,
  key: string,
  filePath: string,
): Promise<void> {
  const logger = getLogger();
  const content = await readFile(filePath);
  const fileStat = await stat(filePath);

  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: content,
      ContentType: 'application/vnd.apache.parquet',
    }),
  );

  logger.info(
    { bucket, key, bytes: fileStat.size },
    'Silver file uploaded',
  );
}