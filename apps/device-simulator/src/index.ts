import { Topics, type TelemetryEvent } from '@iot/contracts';
import { getConfig } from '@iot/config';
import { getLogger, startTracing, simulatorDevicesActive, ingestEventsTotal, kafkaProducerErrors } from '@iot/observability';
import { createDevice, generateTelemetry, type SimulatedDevice } from './device.js';
import { getProducer, disconnectProducer } from './kafka.js';

const DEVICE_COUNT = Number(process.env.DEVICE_COUNT ?? 10);
const INTERVAL_MS = Number(process.env.INTERVAL_MS ?? 1000);

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  logger.info({ deviceCount: DEVICE_COUNT, intervalMs: INTERVAL_MS }, 'Starting device simulator');

  const devices: SimulatedDevice[] = Array.from({ length: DEVICE_COUNT }, (_, i) => createDevice(i + 1));
  simulatorDevicesActive.set(devices.length);

  const producer = await getProducer();

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down simulator');
    await disconnectProducer();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  const tick = async (): Promise<void> => {
    const messages: { key: string; value: string }[] = [];

    for (const device of devices) {
      const event = generateTelemetry(device);
      messages.push({
        key: event.deviceId,
        value: JSON.stringify(event),
      });
    }

    try {
      await producer.send({
        topic: Topics.TELEMETRY_RAW,
        messages,
      });
      for (let i = 0; i < devices.length; i++) {
        ingestEventsTotal.inc({ source: 'simulator', status: 'ok' });
      }
      logger.debug({ count: devices.length }, 'Batch published');
    } catch (err) {
      kafkaProducerErrors.inc({ topic: Topics.TELEMETRY_RAW });
      logger.error({ err }, 'Failed to publish batch');
    }
  };

  // Run immediately, then on interval
  await tick();
  const timer = setInterval(() => void tick(), INTERVAL_MS);

  // Keep the process alive
  process.on('beforeExit', () => clearInterval(timer));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});