import mqtt, { type MqttClient } from 'mqtt';
import { getConfig } from '@iot/config';
import {
  getLogger,
  startTracing,
  simulatorDevicesActive,
  ingestEventsTotal,
} from '@iot/observability';
import { createDevice, generateTelemetry, type SimulatedDevice } from './device.js';

const DEVICE_COUNT = Number(process.env.DEVICE_COUNT ?? 10);
const INTERVAL_MS = Number(process.env.INTERVAL_MS ?? 1000);
const MODE = (process.env.MODE ?? 'http') as 'http' | 'mqtt' | 'both';

async function sendHttp(
  event: unknown,
  apiKey: string,
  baseUrl: string,
  logger: ReturnType<typeof getLogger>,
): Promise<void> {
  try {
    const res = await fetch(`${baseUrl}/ingest`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
      },
      body: JSON.stringify(event),
    });
    if (res.ok) {
      ingestEventsTotal.inc({ source: 'simulator-http', status: 'ok' });
    } else {
      ingestEventsTotal.inc({ source: 'simulator-http', status: `http_${res.status}` });
      logger.warn({ status: res.status }, 'HTTP ingest failed');
    }
  } catch (err) {
    ingestEventsTotal.inc({ source: 'simulator-http', status: 'error' });
    logger.error({ err }, 'HTTP ingest exception');
  }
}

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  logger.info({ deviceCount: DEVICE_COUNT, intervalMs: INTERVAL_MS, mode: MODE }, 'Starting simulator');

  const devices: SimulatedDevice[] = Array.from({ length: DEVICE_COUNT }, (_, i) => createDevice(i + 1));
  simulatorDevicesActive.set(devices.length);

  // MQTT client (shared across all devices for efficiency)
  let mqttClient: MqttClient | undefined;
  if (MODE === 'mqtt' || MODE === 'both') {
    mqttClient = mqtt.connect(`mqtt://localhost:${config.MQTT_TCP_PORT}`, {
      clientId: `simulator-${Date.now()}`,
      clean: true,
      reconnectPeriod: 2000,
    });
    mqttClient.on('connect', () => logger.info('MQTT connected'));
    mqttClient.on('error', (err) => logger.error({ err }, 'MQTT error'));
  }

  const apiKey = config.DEVICE_API_KEYS.split(',')[0]!;
  const httpBase = `http://localhost:${config.INGEST_HTTP_PORT}`;

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down simulator');
    if (mqttClient) await mqttClient.endAsync();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  const tick = async (): Promise<void> => {
    const promises: Promise<void>[] = [];

    for (const device of devices) {
      const event = generateTelemetry(device);

      if ((MODE === 'http' || MODE === 'both') && device.deviceId.charCodeAt(4) % 2 === 0) {
        promises.push(sendHttp(event, apiKey, httpBase, logger));
      }

      if ((MODE === 'mqtt' || MODE === 'both') && mqttClient) {
        const topic = `devices/${device.deviceId}/telemetry`;
        mqttClient.publish(topic, JSON.stringify({ metrics: event.metrics, tags: event.tags }));
      }
    }

    await Promise.all(promises);
    logger.debug({ count: devices.length }, 'Batch published');
  };

  await tick();
  setInterval(() => void tick(), INTERVAL_MS);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});