/**
 * Kafka topic names — centralized so no service hardcodes strings.
 */
export const Topics = {
  TELEMETRY_RAW: 'telemetry.raw',
  TELEMETRY_ALERTS: 'telemetry.alerts',
  TELEMETRY_DLQ: 'telemetry.dlq',
  TELEMETRY_AGGREGATES: 'telemetry.aggregates',
} as const;

export type TopicName = (typeof Topics)[keyof typeof Topics];

/**
 * Consumer group IDs — each independent consumer gets its own group.
 */
export const ConsumerGroups = {
  REALTIME_PROCESSOR: 'iot-realtime-processor',
  BATCH_PROCESSOR: 'iot-batch-processor',
  ARCHIVE_CONSUMER: 'iot-archive-consumer',
} as const;

export type ConsumerGroup = (typeof ConsumerGroups)[keyof typeof ConsumerGroups];