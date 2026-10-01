import { getConfig } from "@iot/config";
import { getLogger, startTracing } from "@iot/observability";
import { ConsumerGroups, Topics } from "@iot/contracts";
import { createConsumer, runConsumer, disconnectConsumer } from "./kafka.js";
import { getDb, closeDb } from "./db.js";
import { BatchWriter } from "./batch-writer.js";
import { handleMessage } from "./handler.js";
import {
  startMetricsServer,
  stopMetricsServer,
} from "./plugins/metrics-server.js";
import { LagMonitor } from "./lag-monitor.js";

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  // Initialize DB
  const sql = getDb();
  await sql`SELECT 1`;
  logger.info("PostgreSQL connection verified");

  // Batch writer: flush every 500 events OR every 5 seconds
  const writer = new BatchWriter(sql, {
    maxBatchSize: 500,
    maxBufferMs: 5000,
  });

  // Kafka consumer with its own group
  const consumer = await createConsumer(ConsumerGroups.BATCH_PROCESSOR);
  await consumer.subscribe({
    topic: Topics.TELEMETRY_RAW,
    fromBeginning: false, // only process new events (old ones are in Redis anyway)
  });
  logger.info({ topic: Topics.TELEMETRY_RAW }, "Subscribed to topic");

  await runConsumer(consumer, (payload) => handleMessage(writer, payload));

  const lagMonitor = new LagMonitor(
    ConsumerGroups.BATCH_PROCESSOR,
    [Topics.TELEMETRY_RAW],
    15000,
  );
  await lagMonitor.start();

  startMetricsServer(3005);
  logger.info("processor-batch ready");

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, "Shutting down processor-batch");
    stopMetricsServer();
    await writer.close();
    await disconnectConsumer();
    await closeDb();
    await lagMonitor.stop();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
