import type { FastifyInstance } from 'fastify';
import { getMetricsRegistry } from '@iot/observability';
import { getRedis } from '../redis-client.js';

export async function healthRoutes(app: FastifyInstance): Promise<void> {
  app.get('/health', async () => ({ status: 'ok', service: 'api-rest' }));

  app.get('/metrics', async (_request, reply) => {
    const registry = getMetricsRegistry();
    reply.header('Content-Type', registry.contentType);
    return registry.metrics();
  });

  app.get('/ready', async (_request, reply) => {
    // Deep readiness: is Redis reachable?
    try {
      await getRedis().ping();
      return { status: 'ready' };
    } catch {
      return reply.code(503).send({ status: 'not_ready' });
    }
  });
}