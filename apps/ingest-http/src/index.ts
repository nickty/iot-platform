import Fastify from 'fastify';
import rateLimit from '@fastify/rate-limit';
import Redis from 'ioredis';
import { getConfig } from '@iot/config';
import { getLogger, startTracing } from '@iot/observability';
import { ingestRoutes } from './routes/ingest.js';
import { metricsPlugin } from './plugins/metrics.js';
import { getProducer, disconnectProducer } from './kafka.js';

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  const app = Fastify({
    logger: false, // we use pino via @iot/observability
    bodyLimit: 1024 * 1024, // 1 MB
    trustProxy: true,
  });

  // Redis-backed rate limiting
  const redis = new Redis({
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    maxRetriesPerRequest: 3,
  });

  await app.register(rateLimit, {
    redis,
    max: config.RATE_LIMIT_MAX,
    timeWindow: config.RATE_LIMIT_WINDOW_MS,
    keyGenerator: (request) => {
      // Rate limit by API key, not IP (NAT-friendly)
      const key = request.headers[config.INGEST_API_KEY_HEADER];
      return typeof key === 'string' ? key : request.ip;
    },
  });

  // Register plugins and routes
  await app.register(metricsPlugin);
  await app.register(ingestRoutes);

  // Warm up Kafka producer
  await getProducer();

  // Start server
  await app.listen({
    port: config.INGEST_HTTP_PORT,
    host: config.INGEST_HTTP_HOST,
  });

  logger.info(
    { port: config.INGEST_HTTP_PORT, host: config.INGEST_HTTP_HOST },
    'ingest-http listening',
  );

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down ingest-http');
    await app.close();
    await disconnectProducer();
    await redis.quit();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});