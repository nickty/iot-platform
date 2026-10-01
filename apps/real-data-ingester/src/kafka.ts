import { Kafka, type Producer } from 'kafkajs';
import { getConfig } from '@iot/config';
import { getLogger } from '@iot/observability';

let producer: Producer | undefined;

export async function getProducer(): Promise<Producer> {
  if (producer) return producer;
  const config = getConfig();
  const logger = getLogger();

  const kafka = new Kafka({
    clientId: `${config.KAFKA_CLIENT_ID}-real-data`,
    brokers: config.KAFKA_BROKERS.split(','),
    retry: { initialRetryTime: 300, retries: 10 },
  });

  producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
  });

  await producer.connect();
  logger.info('Kafka producer connected (real-data-ingester)');
  return producer;
}

export async function disconnectProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = undefined;
  }
}