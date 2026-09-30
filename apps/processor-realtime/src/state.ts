import type Redis from 'ioredis';
import { RedisKeys } from '@iot/contracts';
import type { TelemetryEvent } from '@iot/contracts';

const LATEST_STATE_TTL_SECONDS = 3600;      // 1 hour
const PROCESSED_EVENT_TTL_SECONDS = 86400;  // 24 hours

/**
 * Atomically check if eventId was already processed.
 * Returns true if this is the first time we see it.
 */
export async function markProcessed(
  redis: Redis,
  eventId: string,
): Promise<boolean> {
  const key = RedisKeys.processedEvent(eventId);
  // SET key 1 NX EX ttl  → returns "OK" if set, null if key existed
  const result = await redis.set(key, '1', 'EX', PROCESSED_EVENT_TTL_SECONDS, 'NX');
  return result === 'OK';
}

/**
 * Update latest device state in Redis.
 * Uses a pipeline (single round-trip) for efficiency.
 */
export async function updateLatestState(
  redis: Redis,
  event: TelemetryEvent,
): Promise<void> {
  const key = RedisKeys.deviceLatest(event.deviceId);

  const pipeline = redis.pipeline();
  pipeline.hset(key, {
    deviceId: event.deviceId,
    timestamp: event.timestamp,
    ingestedAt: event.ingestedAt,
    metrics: JSON.stringify(event.metrics),
    tags: JSON.stringify(event.tags ?? {}),
    source: event.source,
    eventId: event.eventId,
  });
  pipeline.expire(key, LATEST_STATE_TTL_SECONDS);

  await pipeline.exec();
}

/**
 * Publish event to a Redis Pub/Sub channel for real-time consumers
 * (e.g., WebSocket servers that will connect on Day 5).
 */
export async function publishLive(
  redis: Redis,
  channel: string,
  payload: unknown,
): Promise<void> {
  await redis.publish(channel, JSON.stringify(payload));
}