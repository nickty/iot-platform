import { z } from 'zod';

/**
 * Metric value — numbers only. Extend later if we need strings/booleans.
 */
export const MetricValueSchema = z.number().finite();

/**
 * The canonical telemetry event envelope.
 * Every event from every protocol gets normalized to this shape.
 */
export const TelemetryEventSchema = z.object({
  eventId: z.string().uuid(),
  deviceId: z.string().min(1).max(128),
  timestamp: z.string().datetime({ offset: true }),
  ingestedAt: z.string().datetime({ offset: true }),
  schemaVersion: z.literal('1.0'),
  metrics: z.record(z.string().min(1).max(64), MetricValueSchema),
  tags: z.record(z.string(), z.string()).optional(),
  source: z.enum(['http', 'mqtt', 'replay']),
});

export type TelemetryEvent = z.infer<typeof TelemetryEventSchema>;

/**
 * Alert event — produced when a threshold is breached.
 */
export const AlertSchema = z.object({
  alertId: z.string().uuid(),
  deviceId: z.string(),
  ruleId: z.string(),
  severity: z.enum(['info', 'warning', 'critical']),
  message: z.string(),
  value: z.number(),
  threshold: z.number(),
  triggeredAt: z.string().datetime({ offset: true }),
});

export type Alert = z.infer<typeof AlertSchema>;


/**
 * Alert rule — defines a threshold on a metric.
 * In production, rules come from a DB. For dev, they're hardcoded.
 */
export const AlertRuleSchema = z.object({
  ruleId: z.string(),
  deviceIdPattern: z.string(),      // exact ID or '*' for all
  metric: z.string(),
  operator: z.enum(['>', '<', '>=', '<=']),
  threshold: z.number(),
  severity: z.enum(['info', 'warning', 'critical']),
  message: z.string(),
});

export type AlertRule = z.infer<typeof AlertRuleSchema>;