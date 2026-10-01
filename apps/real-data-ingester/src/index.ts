import { Topics } from '@iot/contracts';
import { getConfig } from '@iot/config';
import { getLogger, startTracing, ingestEventsTotal } from '@iot/observability';
import { CITIES, fetchWeather } from './sources/open-meteo.js';
import { AIR_QUALITY_LOCATIONS, fetchAirQuality } from './sources/open-aq.js';
import { getProducer, disconnectProducer } from './kafka.js';

const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 60000);

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  logger.info(
    { cities: CITIES.length, aqLocations: AIR_QUALITY_LOCATIONS.length, intervalMs: POLL_INTERVAL_MS },
    'Starting real-data ingester',
  );

  const producer = await getProducer();
  const openAqApiKey = process.env.OPENAQ_API_KEY;

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down real-data ingester');
    await disconnectProducer();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  const tick = async (): Promise<void> => {
    logger.info('Polling real data sources...');

    // Fetch weather for all cities in parallel
    const weatherResults = await Promise.allSettled(
      CITIES.map((city) => fetchWeather(city)),
    );

    // Fetch air quality (only if API key available, else skip)
    const aqResults = openAqApiKey
      ? await Promise.allSettled(
          AIR_QUALITY_LOCATIONS.map((loc) => fetchAirQuality(loc, openAqApiKey)),
        )
      : [];

    const allEvents = [
      ...weatherResults.filter((r): r is PromiseFulfilledResult<NonNullable<Awaited<ReturnType<typeof fetchWeather>>>> => r.status === 'fulfilled' && r.value !== null).map((r) => r.value),
      ...aqResults.filter((r): r is PromiseFulfilledResult<NonNullable<Awaited<ReturnType<typeof fetchAirQuality>>>> => r.status === 'fulfilled' && r.value !== null).map((r) => r.value),
    ];

    if (allEvents.length === 0) {
      logger.warn('No events fetched');
      return;
    }

    try {
      await producer.send({
        topic: Topics.TELEMETRY_RAW,
        messages: allEvents.map((e) => ({
          key: e.deviceId,
          value: JSON.stringify(e),
        })),
      });

      for (let i = 0; i < allEvents.length; i++) {
        ingestEventsTotal.inc({ source: 'real-data', status: 'ok' });
      }

      logger.info({ count: allEvents.length }, 'Real data events published');
    } catch (err) {
      logger.error({ err }, 'Failed to publish real data events');
    }
  };

  await tick();
  setInterval(() => void tick(), POLL_INTERVAL_MS);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});