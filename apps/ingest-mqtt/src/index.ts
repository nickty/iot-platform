import Aedes from 'aedes';
import { createServer, type Server } from 'node:net';
import { getConfig } from '@iot/config';
import { getLogger, startTracing, simulatorDevicesActive } from '@iot/observability';
import { bridgeToKafka } from './bridge.js';
import { getProducer, disconnectProducer } from './kafka.js';

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  const aedes = new Aedes();
  const server: Server = createServer(aedes.handle);

  // Track connected clients
  const clients = new Set<string>();
  aedes.on('client', (client) => {
    clients.add(client.id);
    simulatorDevicesActive.set(clients.size);
    logger.info({ clientId: client.id, total: clients.size }, 'MQTT client connected');
  });
  aedes.on('clientDisconnect', (client) => {
    clients.delete(client.id);
    simulatorDevicesActive.set(clients.size);
    logger.info({ clientId: client.id, total: clients.size }, 'MQTT client disconnected');
  });

  // Bridge published messages to Kafka
  aedes.on('publish', (packet, client) => {
    // Ignore internal aedes messages (client is null on system publishes)
    if (!client) return;
    if (packet.topic.startsWith('$SYS/')) return;

    void bridgeToKafka(packet.topic, packet.payload as Buffer);
  });

  // Warm up Kafka producer
  await getProducer();

  server.listen(config.MQTT_TCP_PORT, config.MQTT_HOST, () => {
    logger.info(
      { port: config.MQTT_TCP_PORT, host: config.MQTT_HOST },
      'ingest-mqtt listening (TCP)',
    );
  });

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down ingest-mqtt');
    server.close();
    await aedes.close();
    await disconnectProducer();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});