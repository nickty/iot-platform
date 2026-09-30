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