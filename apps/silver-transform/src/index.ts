import { S3Client } from '@aws-sdk/client-s3';
import { getConfig } from '@iot/config';
import { getLogger, startTracing } from '@iot/observability';
import { runSilverTransform } from './transform.js';
import { startMetricsServer, stopMetricsServer } from './plugins/metrics-server.js';

async function main(): Promise<void> {
  startTracing();
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

  startMetricsServer(3007);
  logger.info(
    { intervalMs: config.SILVER_INTERVAL_MS, batchSize: config.SILVER_BATCH_SIZE },
    'silver-transform ready',
  );

  let running = false;

  const tick = async (): Promise<void> => {
    if (running) return;
    running = true;
    try {
      await runSilverTransform(s3);
    } catch (err) {
      logger.error({ err }, 'Silver transform failed');
    } finally {
      running = false;
    }
  };

  // Run immediately, then on interval
  await tick();
  const timer = setInterval(() => void tick(), config.SILVER_INTERVAL_MS);

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down silver-transform');
    clearInterval(timer);
    stopMetricsServer();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});