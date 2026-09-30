import type { FastifyInstance } from 'fastify';
import { RedisKeys } from '@iot/contracts';
import { getRedis } from '../redis-client.js';

/**
 * Parse a Redis hash into a device state object.
 */
function parseDeviceState(hash: Record<string, string>): {
  deviceId: string;
  timestamp: string;
  ingestedAt: string;
  metrics: Record<string, number>;
  tags: Record<string, string>;
  source: string;
} | null {
  if (!hash.deviceId) return null;

  return {
    deviceId: hash.deviceId,
    timestamp: hash.timestamp ?? '',
    ingestedAt: hash.ingestedAt ?? '',
    metrics: hash.metrics ? JSON.parse(hash.metrics) : {},
    tags: hash.tags ? JSON.parse(hash.tags) : {},
    source: hash.source ?? 'unknown',
  };
}

export async function deviceRoutes(app: FastifyInstance): Promise<void> {
  const redis = getRedis();

  // GET /devices — list all known devices with their latest state
  app.get('/devices', async (_request, reply) => {
    // SCAN is safer than KEYS in production (non-blocking)
    const keys: string[] = [];
    let cursor = '0';
    do {
      const [nextCursor, batch] = await redis.scan(cursor, 'MATCH', 'device:*:latest', 'COUNT', 100);
      cursor = nextCursor;
      keys.push(...batch);
    } while (cursor !== '0');

    if (keys.length === 0) {
      return reply.send({ devices: [], count: 0 });
    }

    const pipeline = redis.pipeline();
    for (const key of keys) pipeline.hgetall(key);
    const results = await pipeline.exec();

    const devices = (results ?? [])
      .map(([err, hash]) => {
        if (err || !hash) return null;
        return parseDeviceState(hash as Record<string, string>);
      })
      .filter((d) => d !== null);

    return reply.send({ devices, count: devices.length });
  });

  // GET /devices/:id — latest state of a single device
  app.get<{ Params: { id: string } }>('/devices/:id', async (request, reply) => {
    const { id } = request.params;
    const hash = await redis.hgetall(RedisKeys.deviceLatest(id));

    if (!hash || Object.keys(hash).length === 0) {
      return reply.code(404).send({
        error: 'not_found',
        message: `Device ${id} has no recent state`,
      });
    }

    const device = parseDeviceState(hash);
    if (!device) {
      return reply.code(500).send({ error: 'parse_error' });
    }

    return reply.send(device);
  });
}