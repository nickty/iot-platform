import { Registry, Counter, Histogram, Gauge, collectDefaultMetrics } from 'prom-client';

let registry: Registry | undefined;

export function getMetricsRegistry(): Registry {
  if (registry) return registry;

  registry = new Registry();
  registry.setDefaultLabels({ app: 'iot-platform' });
  collectDefaultMetrics({ register: registry });
  return registry;
}

// Common metrics — reuse across services
export const ingestEventsTotal = new Counter({
  name: 'ingest_events_total',
  help: 'Total events ingested',
  labelNames: ['source', 'status'] as const,
  registers: [getMetricsRegistry()],
});

export const ingestLatency = new Histogram({
  name: 'ingest_latency_seconds',
  help: 'Ingest request latency',
  labelNames: ['source', 'endpoint'] as const,
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
  registers: [getMetricsRegistry()],
});

export const kafkaProducerErrors = new Counter({
  name: 'kafka_producer_errors_total',
  help: 'Kafka producer errors',
  labelNames: ['topic'] as const,
  registers: [getMetricsRegistry()],
});

export const simulatorDevicesActive = new Gauge({
  name: 'simulator_devices_active',
  help: 'Number of active simulated devices',
  registers: [getMetricsRegistry()],
});




export const processorEventsProcessedTotal = new Counter({
  name: 'processor_events_processed_total',
  help: 'Events processed by realtime processor',
  labelNames: ['status'] as const,
  registers: [getMetricsRegistry()],
});

export const processorEventDuration = new Histogram({
  name: 'processor_event_duration_seconds',
  help: 'Time spent processing a single event',
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
  registers: [getMetricsRegistry()],
});

export const processorAlertsFiredTotal = new Counter({
  name: 'processor_alerts_fired_total',
  help: 'Alerts fired by severity',
  labelNames: ['severity'] as const,
  registers: [getMetricsRegistry()],
});

export const processorDuplicatesTotal = new Counter({
  name: 'processor_duplicates_total',
  help: 'Duplicate events skipped',
  registers: [getMetricsRegistry()],
});

export const processorDlqTotal = new Counter({
  name: 'processor_dlq_total',
  help: 'Messages sent to DLQ by reason',
  labelNames: ['reason'] as const,
  registers: [getMetricsRegistry()],
});


export const wsConnectionsActive = new Gauge({
  name: 'websocket_connections_active',
  help: 'Active WebSocket connections',
  registers: [getMetricsRegistry()],
});

export const wsMessagesSent = new Counter({
  name: 'websocket_messages_sent_total',
  help: 'WebSocket messages sent',
  labelNames: ['type'] as const,
  registers: [getMetricsRegistry()],
});


export const batchEventsInsertedTotal = new Counter({
  name: 'batch_events_inserted_total',
  help: 'Events inserted into TimescaleDB',
  registers: [getMetricsRegistry()],
});

export const batchEventsInvalidTotal = new Counter({
  name: 'batch_events_invalid_total',
  help: 'Events rejected by batch processor',
  labelNames: ['reason'] as const,
  registers: [getMetricsRegistry()],
});

export const batchFlushDuration = new Histogram({
  name: 'batch_flush_duration_seconds',
  help: 'Time to buffer (not insert) an event',
  buckets: [0.0001, 0.0005, 0.001, 0.005, 0.01],
  registers: [getMetricsRegistry()],
});


export const kafkaConsumerLag = new Gauge({
  name: 'kafka_consumer_lag',
  help: 'Kafka consumer group lag (messages behind the latest offset)',
  labelNames: ['group', 'topic', 'partition'] as const,
  registers: [getMetricsRegistry()],
});


export const archiveEventsBufferedTotal = new Counter({
  name: 'archive_events_buffered_total',
  help: 'Events buffered by archive consumer',
  registers: [getMetricsRegistry()],
});

export const archiveFilesWrittenTotal = new Counter({
  name: 'archive_files_written_total',
  help: 'Parquet files written to S3',
  labelNames: ['zone'] as const,
  registers: [getMetricsRegistry()],
});

export const archiveBytesWrittenTotal = new Counter({
  name: 'archive_bytes_written_total',
  help: 'Total bytes written to S3',
  registers: [getMetricsRegistry()],
});

export const archiveFlushDuration = new Histogram({
  name: 'archive_flush_duration_seconds',
  help: 'Time to write a Parquet file to S3',
  buckets: [0.1, 0.5, 1, 2, 5, 10],
  registers: [getMetricsRegistry()],
});

export const archiveBufferSize = new Gauge({
  name: 'archive_buffer_size',
  help: 'Current number of events in memory buffer',
  registers: [getMetricsRegistry()],
});


export const silverFilesProcessedTotal = new Counter({
  name: 'silver_files_processed_total',
  help: 'Bronze files processed into Silver',
  registers: [getMetricsRegistry()],
});

export const silverEventsInTotal = new Counter({
  name: 'silver_events_in_total',
  help: 'Events read from Bronze',
  registers: [getMetricsRegistry()],
});

export const silverEventsOutTotal = new Counter({
  name: 'silver_events_out_total',
  help: 'Events written to Silver',
  registers: [getMetricsRegistry()],
});

export const silverDuplicatesRemovedTotal = new Counter({
  name: 'silver_duplicates_removed_total',
  help: 'Duplicate events removed during transform',
  registers: [getMetricsRegistry()],
});

export const silverTransformDuration = new Histogram({
  name: 'silver_transform_duration_seconds',
  help: 'Time to complete a full Silver transform',
  buckets: [0.5, 1, 2, 5, 10, 30, 60],
  registers: [getMetricsRegistry()],
});