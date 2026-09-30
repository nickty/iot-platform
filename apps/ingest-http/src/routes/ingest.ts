import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { z } from 'zod';
import { v7 as uuidv7 } from 'uuid';
import { Topics, TelemetryEventSchema, type TelemetryEvent } from '@iot/contracts';
import { getLogger, ingestEventsTotal, ingestLatency } from '@iot/observability';
import { apiKeyAuth } from '../auth.js';
import { getProducer } from '../kafka.js';

/**
 * Input schema for HTTP ingestion.
 * We accept partial telemetry and add server-side fields (eventId, ingestedAt).
 */
const IngestBodySchema = z.object({
  deviceId: z.string().min(1).max(128),
  timestamp: z.string().datetime({ offset: true }).optional(),
  metrics: z.record(z.string(), z.number().finite()),
  tags: z.record(z.string(), z.string()).optional(),
});

const BatchBodySchema = z.object({
  events: z.array(IngestBodySchema).min(1).max(1000),
});

export async function ingestRoutes(app: FastifyInstance): Promise<void> {
  // Single event ingest
  app.post(
    '/ingest',
    { preHandler: apiKeyAuth },
    async (request: FastifyRequest, reply: FastifyReply) => {
      const startTime = performance.now();
      const logger = getLogger();

      const parsed = IngestBodySchema.safeParse(request.body);
      if (!parsed.success) {
        ingestEventsTotal.inc({ source: 'http', status: 'invalid' });
        reply.code(400).send({
          error: 'validation_failed',
          details: parsed.error.format(),
        });
        return;
      }

      const event = normalizeEvent(parsed.data);

      try {
        const producer = await getProducer();
        await producer.send({
          topic: Topics.TELEMETRY_RAW,
          messages: [{ key: event.deviceId, value: JSON.stringify(event) }],
        });

        ingestEventsTotal.inc({ source: 'http', status: 'ok' });
        ingestLatency.observe(
          { source: 'http', endpoint: '/ingest' },
          (performance.now() - startTime) / 1000,
        );

        reply.code(202).send({ eventId: event.eventId, status: 'accepted' });
      } catch (err) {
        logger.error({ err, deviceId: event.deviceId }, 'Failed to publish event');
        ingestEventsTotal.inc({ source: 'http', status: 'error' });
        reply.code(503).send({
          error: 'service_unavailable',
          message: 'Failed to enqueue event',
        });
      }
    },
  );

  // Batch ingest (more efficient for high-volume devices)
  app.post(
    '/ingest/batch',
    { preHandler: apiKeyAuth },
    async (request: FastifyRequest, reply: FastifyReply) => {
      const startTime = performance.now();
      const logger = getLogger();

      const parsed = BatchBodySchema.safeParse(request.body);
      if (!parsed.success) {
        ingestEventsTotal.inc({ source: 'http', status: 'invalid' });
        reply.code(400).send({
          error: 'validation_failed',
          details: parsed.error.format(),
        });
        return;
      }

      const events = parsed.data.events.map(normalizeEvent);

      try {
        const producer = await getProducer();
        await producer.send({
          topic: Topics.TELEMETRY_RAW,
          messages: events.map((e) => ({
            key: e.deviceId,
            value: JSON.stringify(e),
          })),
        });

        ingestEventsTotal.inc({ source: 'http', status: 'ok' }, events.length);
        ingestLatency.observe(
          { source: 'http', endpoint: '/ingest/batch' },
          (performance.now() - startTime) / 1000,
        );

        reply.code(202).send({
          accepted: events.length,
          status: 'accepted',
        });
      } catch (err) {
        logger.error({ err, count: events.length }, 'Failed to publish batch');
        ingestEventsTotal.inc({ source: 'http', status: 'error' }, events.length);
        reply.code(503).send({
          error: 'service_unavailable',
          message: 'Failed to enqueue events',
        });
      }
    },
  );
}

/**
 * Convert incoming HTTP payload to canonical TelemetryEvent.
 * Server assigns eventId (UUID v7) and ingestedAt.
 */
function normalizeEvent(input: z.infer<typeof IngestBodySchema>): TelemetryEvent {
  const now = new Date().toISOString();
  const event = {
    eventId: uuidv7(),
    deviceId: input.deviceId,
    timestamp: input.timestamp ?? now,
    ingestedAt: now,
    schemaVersion: '1.0' as const,
    metrics: input.metrics,
    tags: input.tags,
    source: 'http' as const,
  };
  // Validate once more to guarantee shape
  return TelemetryEventSchema.parse(event);
}