import { startMetricsServer, stopMetricsServer } from './plugins/metrics-server.js';
import Redis from 'ioredis';
import { getConfig } from '@iot/config';
import { getLogger, startTracing } from '@iot/observability';
import { ConsumerGroups, Topics } from '@iot/contracts';
import { getProducer, createConsumer, runConsumer, disconnectAll } from './kafka.js';
import { handleMessage } from './handler.js';


async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  // Redis client
  const redis = new Redis({
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    maxRetriesPerRequest: 3,
  });

  // 1. Warm up Kafka producer
  const producer = await getProducer();

  // 2. Connect consumer — does NOT subscribe or run yet
  const consumer = await createConsumer(ConsumerGroups.REALTIME_PROCESSOR);

  // 3. Subscribe to the topic (must happen before run)
  await consumer.subscribe({
    topic: Topics.TELEMETRY_RAW,
    fromBeginning: true,
  });
  logger.info({ topic: Topics.TELEMETRY_RAW }, 'Subscribed to topic');

  // 4. Start consuming
  await runConsumer(consumer, (payload) => handleMessage({ redis, producer }, payload));
  // Start metrics HTTP server
  startMetricsServer(3002);
  logger.info('processor-realtime ready');

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down processor-realtime');
    stopMetricsServer();
    await disconnectAll();
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