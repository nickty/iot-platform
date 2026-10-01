import { S3Client } from '@aws-sdk/client-s3';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';
import { runSilverTransform } from './transform.js';

async function main(): Promise<void> {
  const config = getConfig();
  const logger = getLogger();

  const s3 = new S3Client({
    endpoint: config.S3_ENDPOINT,
    region: config.S3_REGION,
    credentials: {
      accessKeyId: config.S3_ACCESS_KEY,
      secretAccessKey: config.S3_SECRET_KEY,
    },
    forcePathStyle: config.S3_FORCE_PATH_STYLE,
  });

  logger.info('Running silver-transform once');
  const result = await runSilverTransform(s3);
  logger.info(result, 'Done');
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});