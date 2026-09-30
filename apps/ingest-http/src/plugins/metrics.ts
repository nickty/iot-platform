import type { FastifyInstance } from 'fastify';
import { getMetricsRegistry } from '@iot/observability';

export async function metricsPlugin(app: FastifyInstance): Promise<void> {
  app.get('/metrics', async (_request, reply) => {
    const registry = getMetricsRegistry();
    reply.header('Content-Type', registry.contentType);
    return registry.metrics();
  });

  app.get('/health', async () => ({ status: 'ok', service: 'ingest-http' }));
}