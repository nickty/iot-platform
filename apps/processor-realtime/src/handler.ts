import type Redis from "ioredis";
import type { Producer, EachMessagePayload } from "kafkajs";
import { v7 as uuidv7 } from "uuid";
import {
  TelemetryEventSchema,
  Topics,
  RedisChannels,
  type TelemetryEvent,
} from "@iot/contracts";
import {
  getLogger,
  processorEventsProcessedTotal,
  processorEventDuration,
  processorAlertsFiredTotal,
  processorDuplicatesTotal,
  processorDlqTotal,
} from "@iot/observability";
import { evaluateRules } from "./rules.js";
import { markProcessed, updateLatestState, publishLive } from "./state.js";
import { detectAnomaly } from "./anomaly.js";
import { processorAnomaliesDetectedTotal } from "@iot/observability";

export interface HandlerDeps {
  redis: Redis;
  producer: Producer;
}

/**
 * The main per-message handler.
 * This is called for EVERY message in telemetry.raw.
 */
export async function handleMessage(
  deps: HandlerDeps,
  payload: EachMessagePayload,
): Promise<void> {
  const { redis, producer } = deps;
  const logger = getLogger();
  const startTime = performance.now();

  const raw = payload.message.value?.toString("utf-8");
  if (!raw) {
    logger.warn("Received empty message — skipping");
    return;
  }

  // ── Step 1: Parse JSON ───────────────────────────────────────────
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    logger.warn(
      { err, raw: raw.slice(0, 200) },
      "Invalid JSON — sending to DLQ",
    );
    await sendToDlq(producer, raw, "invalid_json", err);
    processorDlqTotal.inc({ reason: "invalid_json" });
    return;
  }

  // ── Step 2: Validate schema ─────────────────────────────────────
  const validated = TelemetryEventSchema.safeParse(parsed);
  if (!validated.success) {
    logger.warn(
      { errors: validated.error.format(), raw: raw.slice(0, 200) },
      "Schema validation failed — sending to DLQ",
    );
    await sendToDlq(producer, raw, "schema_invalid", validated.error);
    processorDlqTotal.inc({ reason: "schema_invalid" });
    return;
  }
  const event: TelemetryEvent = validated.data;

  // ── Step 3: Idempotency check ───────────────────────────────────
  const isFirstTime = await markProcessed(redis, event.eventId);
  if (!isFirstTime) {
    logger.debug({ eventId: event.eventId }, "Duplicate — skipping");
    processorDuplicatesTotal.inc();
    return;
  }

  // ── Step 4: Update latest state in Redis ────────────────────────
  await updateLatestState(redis, event);

  // ── Step 5: Evaluate alert rules ────────────────────────────────
  const breaches = evaluateRules(event.deviceId, event.metrics);

  for (const { rule, value } of breaches) {
    const alert = {
      alertId: uuidv7(),
      deviceId: event.deviceId,
      ruleId: rule.ruleId,
      severity: rule.severity,
      message: rule.message,
      value,
      threshold: rule.threshold,
      triggeredAt: new Date().toISOString(),
    };

    await producer.send({
      topic: Topics.TELEMETRY_ALERTS,
      messages: [
        {
          key: event.deviceId,
          value: JSON.stringify(alert),
        },
      ],
    });

    await publishLive(redis, RedisChannels.alertsLive, alert);
    processorAlertsFiredTotal.inc({ severity: rule.severity });
    logger.warn({ alert }, "Alert fired");
  }
  // ── Step 5b: Anomaly detection (Z-score) ───────────────────────
  for (const [metric, value] of Object.entries(event.metrics)) {
    const result = await detectAnomaly(redis, event.deviceId, metric, value);
    if (result?.isAnomaly) {
      const alert = {
        alertId: uuidv7(),
        deviceId: event.deviceId,
        ruleId: `anomaly-${metric}`,
        severity: "warning" as const,
        message: `Anomaly detected in ${metric}: value=${value.toFixed(2)}, z-score=${result.zScore.toFixed(2)}`,
        value,
        threshold: result.mean + 3 * result.stdDev,
        triggeredAt: new Date().toISOString(),
      };

      await producer.send({
        topic: Topics.TELEMETRY_ALERTS,
        messages: [{ key: event.deviceId, value: JSON.stringify(alert) }],
      });

      await publishLive(redis, RedisChannels.alertsLive, alert);
      processorAnomaliesDetectedTotal.inc({ metric });
      logger.warn({ alert, zScore: result.zScore }, "Anomaly detected");
    }
  }
  // ── Step 6: Publish to Redis Pub/Sub for WebSocket fan-out ──────
  await publishLive(redis, RedisChannels.telemetryLive, {
    deviceId: event.deviceId,
    metrics: event.metrics,
    tags: event.tags,
    timestamp: event.timestamp,
  });

  // ── Step 7: Metrics ─────────────────────────────────────────────
  const durationSeconds = (performance.now() - startTime) / 1000;
  processorEventDuration.observe(durationSeconds);
  processorEventsProcessedTotal.inc({ status: "ok" });

  logger.debug(
    {
      deviceId: event.deviceId,
      durationMs: durationSeconds * 1000,
      alerts: breaches.length,
    },
    "Event processed",
  );
}

/**
 * Send a failed message to the DLQ topic for later inspection.
 */
async function sendToDlq(
  producer: Producer,
  raw: string,
  reason: string,
  error: unknown,
): Promise<void> {
  try {
    await producer.send({
      topic: Topics.TELEMETRY_DLQ,
      messages: [
        {
          value: JSON.stringify({
            reason,
            error: String(error),
            raw: raw.slice(0, 1000),
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
  } catch (err) {
    console.error("Failed to send to DLQ", err);
  }
}
