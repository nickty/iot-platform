import { Topics } from '@iot/contracts';
import { getConfig } from '@iot/config';
import { getLogger, startTracing, ingestEventsTotal } from '@iot/observability';
import { CITIES, fetchWeather } from './sources/open-meteo.js';
import { getProducer, disconnectProducer } from './kafka.js';

const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 60000); // 1 minute

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  logger.info({ cities: CITIES.length, intervalMs: POLL_INTERVAL_MS }, 'Starting real-data ingester');

  const producer = await getProducer();

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down real-data ingester');
    await disconnectProducer();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  const tick = async (): Promise<void> => {
    logger.info('Polling weather data...');

    const results = await Promise.allSettled(
      CITIES.map((city) => fetchWeather(city)),
    );

    const events = results
      .filter((r): r is PromiseFulfilledResult<NonNullable<Awaited<ReturnType<typeof fetchWeather>>>> => r.status === 'fulfilled' && r.value !== null)
      .map((r) => r.value);

    if (events.length === 0) {
      logger.warn('No events fetched');
      return;
    }

    try {
      await producer.send({
        topic: Topics.TELEMETRY_RAW,
        messages: events.map((e) => ({
          key: e.deviceId,
          value: JSON.stringify(e),
        })),
      });

      for (let i = 0; i < events.length; i++) {
        ingestEventsTotal.inc({ source: 'real-data', status: 'ok' });
      }

      logger.info({ count: events.length }, 'Weather events published');
    } catch (err) {
      logger.error({ err }, 'Failed to publish weather events');
    }
  };

  // Run immediately, then on interval
  await tick();
  setInterval(() => void tick(), POLL_INTERVAL_MS);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});