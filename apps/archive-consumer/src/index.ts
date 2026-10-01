import { getConfig } from '@iot/config';
import { getLogger, startTracing, archiveEventsBufferedTotal } from '@iot/observability';
import { ConsumerGroups, Topics, TelemetryEventSchema } from '@iot/contracts';
import { createConsumer, runConsumer, disconnectConsumer } from './kafka.js';
import { BufferedWriter } from './buffered-writer.js';
import { startMetricsServer, stopMetricsServer } from './plugins/metrics-server.js';
import type { EachMessagePayload } from 'kafkajs';

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  // Buffered writer: flush every 5000 events OR every 60 seconds
  const writer = new BufferedWriter({
    maxBatchSize: 5000,
    maxBufferMs: 60000,
    zone: 'telemetry',       // Bronze layer
  });

  // Kafka consumer
  const consumer = await createConsumer(ConsumerGroups.ARCHIVE_CONSUMER);
  await consumer.subscribe({
    topic: Topics.TELEMETRY_RAW,
    fromBeginning: false,
  });
  logger.info({ topic: Topics.TELEMETRY_RAW }, 'Subscribed to topic');

  await runConsumer(consumer, async (payload: EachMessagePayload) => {
    const raw = payload.message.value?.toString('utf-8');
    if (!raw) return;

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return;
    }

    const validated = TelemetryEventSchema.safeParse(parsed);
    if (!validated.success) return;

    writer.add(validated.data);
    archiveEventsBufferedTotal.inc();
  });

  startMetricsServer(3006);
  logger.info('archive-consumer ready');

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down archive-consumer');
    stopMetricsServer();
    await writer.close();
    await disconnectConsumer();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});