import type { FastifyInstance } from "fastify";
import { RedisKeys } from "@iot/contracts";
import { getRedis } from "../redis-client.js";
import { getDb } from "../db.js";

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
    timestamp: hash.timestamp ?? "",
    ingestedAt: hash.ingestedAt ?? "",
    metrics: hash.metrics ? JSON.parse(hash.metrics) : {},
    tags: hash.tags ? JSON.parse(hash.tags) : {},
    source: hash.source ?? "unknown",
  };
}

export async function deviceRoutes(app: FastifyInstance): Promise<void> {
  const sql = getDb();
  const redis = getRedis();

  // GET /devices — list all known devices with their latest state
  app.get("/devices", async (_request, reply) => {
    // SCAN is safer than KEYS in production (non-blocking)
    const keys: string[] = [];
    let cursor = "0";
    do {
      const [nextCursor, batch] = await redis.scan(
        cursor,
        "MATCH",
        "device:*:latest",
        "COUNT",
        100,
      );
      cursor = nextCursor;
      keys.push(...batch);
    } while (cursor !== "0");

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
  app.get<{ Params: { id: string } }>(
    "/devices/:id",
    async (request, reply) => {
      const { id } = request.params;
      const hash = await redis.hgetall(RedisKeys.deviceLatest(id));

      if (!hash || Object.keys(hash).length === 0) {
        return reply.code(404).send({
          error: "not_found",
          message: `Device ${id} has no recent state`,
        });
      }

      const device = parseDeviceState(hash);
      if (!device) {
        return reply.code(500).send({ error: "parse_error" });
      }

      return reply.send(device);
    },
  );

  // GET /devices/:id/history — time-series query against TimescaleDB
  app.get<{
    Params: { id: string };
    Querystring: {
      from?: string;
      to?: string;
      interval?: "1m" | "5m" | "1h" | "raw";
      limit?: string;
    };
  }>("/devices/:id/history", async (request, reply) => {
    const { id } = request.params;
    const { from, to, interval = "raw", limit = "1000" } = request.query;

    const toDate = to ? new Date(to) : new Date();
    const fromDate = from
      ? new Date(from)
      : new Date(Date.now() - 60 * 60 * 1000); // default 1h

    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      return reply.code(400).send({ error: "invalid_date_range" });
    }

    const limitNum = Math.min(parseInt(limit, 10) || 1000, 10000);

    let rows: unknown[];
    try {
      if (interval === "raw") {
        rows = await sql`
          SELECT
            time,
            (metrics->>'temperature')::float AS temperature,
            (metrics->>'humidity')::float AS humidity,
            (metrics->>'pressure')::float AS pressure
          FROM telemetry
          WHERE device_id = ${id}
            AND time >= ${fromDate}
            AND time <= ${toDate}
          ORDER BY time ASC
          LIMIT ${limitNum}
        `;
      } else {
        // Use continuous aggregate
        const view =
          interval === "1m"
            ? sql`telemetry_1m`
            : interval === "5m"
              ? sql`telemetry_5m`
              : sql`telemetry_1h`;

        rows = await sql`
          SELECT
            bucket AS time,
            avg_temperature AS temperature,
            avg_humidity AS humidity,
            avg_pressure AS pressure,
            sample_count
          FROM ${view}
          WHERE device_id = ${id}
            AND bucket >= ${fromDate}
            AND bucket <= ${toDate}
          ORDER BY bucket ASC
          LIMIT ${limitNum}
        `;
      }
    } catch (err) {
      return reply
        .code(500)
        .send({ error: "query_failed", message: String(err) });
    }

    return reply.send({
      deviceId: id,
      interval,
      from: fromDate.toISOString(),
      to: toDate.toISOString(),
      count: rows.length,
      data: rows,
    });
  });
}
