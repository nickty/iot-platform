import Fastify from 'fastify';
import { getConfig } from '@iot/config';
import { getLogger, startTracing } from '@iot/observability';
import { deviceRoutes } from './routes/devices.js';
import { healthRoutes } from './routes/health.js';
import { getRedis, closeRedis } from './redis-client.js';

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  const app = Fastify({
    logger: false,
    trustProxy: true,
  });

  // Warm up Redis before accepting traffic
  await getRedis().ping();

  await app.register(healthRoutes);
  await app.register(deviceRoutes);

  await app.listen({
    port: 3003,
    host: '0.0.0.0',
  });

  logger.info({ port: 3003 }, 'api-rest listening');

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down api-rest');
    await app.close();
    await closeRedis();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});