import type { FastifyRequest, FastifyReply } from 'fastify';
import { getConfig } from '@iot/config';

/**
 * API key authentication.
 * In production, keys would be hashed and stored in a DB.
 * For dev, we read a comma-separated list from env.
 */
export async function apiKeyAuth(
  request: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  const config = getConfig();
  const headerName = config.INGEST_API_KEY_HEADER;

  const apiKey = request.headers[headerName];
  if (!apiKey || typeof apiKey !== 'string') {
    reply.code(401).send({
      error: 'unauthorized',
      message: `Missing ${headerName} header`,
    });
    return;
  }

  const validKeys = config.DEVICE_API_KEYS.split(',').map((k) => k.trim());
  if (!validKeys.includes(apiKey)) {
    reply.code(403).send({
      error: 'forbidden',
      message: 'Invalid API key',
    });
    return;
  }
}