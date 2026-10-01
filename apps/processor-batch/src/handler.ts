import type { EachMessagePayload } from 'kafkajs';
import { TelemetryEventSchema } from '@iot/contracts';
import {
  getLogger,
  batchEventsInsertedTotal,
  batchEventsInvalidTotal,
  batchFlushDuration,
} from '@iot/observability';
import type { BatchWriter } from './batch-writer.js';

export async function handleMessage(
  writer: BatchWriter,
  payload: EachMessagePayload,
): Promise<void> {
  const logger = getLogger();
  const raw = payload.message.value?.toString('utf-8');
  if (!raw) return;

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    batchEventsInvalidTotal.inc({ reason: 'invalid_json' });
    return;
  }

  const validated = TelemetryEventSchema.safeParse(parsed);
  if (!validated.success) {
    batchEventsInvalidTotal.inc({ reason: 'schema_invalid' });
    return;
  }

  const flushStart = performance.now();
  await writer.add(validated.data);
  batchEventsInsertedTotal.inc();
  batchFlushDuration.observe((performance.now() - flushStart) / 1000);

  logger.debug({ deviceId: validated.data.deviceId }, 'Event buffered');
}