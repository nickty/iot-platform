import { Kafka, type Producer, type Consumer, type EachMessagePayload } from 'kafkajs';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';

let producer: Producer | undefined;
let consumer: Consumer | undefined;

function createKafka(): Kafka {
  const config = getConfig();
  return new Kafka({
    clientId: `${config.KAFKA_CLIENT_ID}-processor-realtime`,
    brokers: config.KAFKA_BROKERS.split(','),
    retry: { initialRetryTime: 300, retries: 10 },
  });
}

export async function getProducer(): Promise<Producer> {
  if (producer) return producer;
  const logger = getLogger();
  producer = createKafka().producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
  });
  await producer.connect();
  logger.info('Kafka producer connected (processor-realtime)');
  return producer;
}

/**
 * Create and connect a consumer. Does NOT subscribe or run — the caller does that.
 * This ensures correct lifecycle ordering: connect → subscribe → run.
 */
export async function createConsumer(groupId: string): Promise<Consumer> {
  if (consumer) return consumer;
  const logger = getLogger();

  consumer = createKafka().consumer({
    groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576,
    maxWaitTimeInMs: 500,
  });

  await consumer.connect();
  logger.info({ groupId }, 'Kafka consumer connected');
  return consumer;
}

/**
 * Start consuming. Called AFTER subscribe().
 */
export async function runConsumer(
  c: Consumer,
  handler: (payload: EachMessagePayload) => Promise<void>,
): Promise<void> {
  const logger = getLogger();
  await c.run({
    eachMessage: async (payload) => {
      try {
        await handler(payload);
      } catch (err) {
        logger.error(
          {
            err,
            topic: payload.topic,
            partition: payload.partition,
            offset: payload.message.offset,
          },
          'Handler failed — message will be retried on next run',
        );
      }
    },
  });
}

export async function disconnectAll(): Promise<void> {
  if (consumer) { await consumer.disconnect(); consumer = undefined; }
  if (producer) { await producer.disconnect(); producer = undefined; }
}