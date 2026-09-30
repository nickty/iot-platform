import Redis from 'ioredis';
import { getConfig } from '@iot/config';

let client: Redis | undefined;

export function getRedis(): Redis {
  if (client) return client;
  const config = getConfig();
  client = new Redis({
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    maxRetriesPerRequest: 3,
  });
  return client;
}

export async function closeRedis(): Promise<void> {
  if (client) {
    await client.quit();
    client = undefined;
  }
}