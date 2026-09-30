import { v7 as uuidv7 } from 'uuid';
import { Topics, TelemetryEventSchema, type TelemetryEvent } from '@iot/contracts';
import { getLogger, ingestEventsTotal, kafkaProducerErrors } from '@iot/observability';
import { getProducer } from './kafka.js';

/**
 * Parse MQTT topic: devices/{deviceId}/telemetry
 */
function parseDeviceId(topic: string): string | null {
  const parts = topic.split('/');
  if (parts.length !== 3 || parts[0] !== 'devices' || parts[2] !== 'telemetry') {
    return null;
  }
  return parts[1] ?? null;
}

/**
 * Convert MQTT payload to canonical TelemetryEvent.
 * Payload expected: { metrics: {...}, tags?: {...}, timestamp?: ISO8601 }
 */
export async function bridgeToKafka(
  topic: string,
  payload: Buffer,
): Promise<void> {
  const logger = getLogger();

  const deviceId = parseDeviceId(topic);
  if (!deviceId) {
    logger.warn({ topic }, 'Ignoring message from unknown topic pattern');
    return;
  }

  let body: unknown;
  try {
    body = JSON.parse(payload.toString('utf-8'));
  } catch {
    ingestEventsTotal.inc({ source: 'mqtt', status: 'invalid' });
    logger.warn({ topic, deviceId }, 'Invalid JSON payload');
    return;
  }

  const now = new Date().toISOString();
  const parsed = TelemetryEventSchema.safeParse({
    eventId: uuidv7(),
    deviceId,
    timestamp:
      typeof body === 'object' && body && 'timestamp' in body
        ? (body as { timestamp: string }).timestamp
        : now,
    ingestedAt: now,
    schemaVersion: '1.0',
    metrics:
      typeof body === 'object' && body && 'metrics' in body
        ? (body as { metrics: Record<string, number> }).metrics
        : {},
    tags:
      typeof body === 'object' && body && 'tags' in body
        ? (body as { tags: Record<string, string> }).tags
        : undefined,
    source: 'mqtt',
  });

  if (!parsed.success) {
    ingestEventsTotal.inc({ source: 'mqtt', status: 'invalid' });
    logger.warn({ topic, deviceId, err: parsed.error.format() }, 'Schema validation failed');
    return;
  }

  const event: TelemetryEvent = parsed.data;

  try {
    const producer = await getProducer();
    await producer.send({
      topic: Topics.TELEMETRY_RAW,
      messages: [{ key: event.deviceId, value: JSON.stringify(event) }],
    });
    ingestEventsTotal.inc({ source: 'mqtt', status: 'ok' });
  } catch (err) {
    kafkaProducerErrors.inc({ topic: Topics.TELEMETRY_RAW });
    ingestEventsTotal.inc({ source: 'mqtt', status: 'error' });
    logger.error({ err, deviceId }, 'Failed to bridge MQTT message to Kafka');
  }
}