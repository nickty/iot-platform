import { createServer } from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import Redis from 'ioredis';
import { getConfig } from '@iot/config';
import { getLogger, startTracing, wsConnectionsActive, wsMessagesSent } from '@iot/observability';
import { RedisChannels } from '@iot/contracts';

interface Client {
  ws: WebSocket;
  subscriptions: Set<string>; // deviceIds or '*'
}

async function main(): Promise<void> {
  startTracing();
  const config = getConfig();
  const logger = getLogger();

  const PORT = 3004;

  // ── WebSocket server ─────────────────────────────────────────────
  const httpServer = createServer((req, res) => {
    if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', service: 'api-websocket' }));
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
  const clients = new Set<Client>();

  wss.on('connection', (ws) => {
    const client: Client = { ws, subscriptions: new Set(['*']) };
    clients.add(client);
    wsConnectionsActive.set(clients.size);
    logger.info({ total: clients.size }, 'WebSocket client connected');

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'subscribe' && typeof msg.deviceId === 'string') {
          client.subscriptions.add(msg.deviceId);
          ws.send(JSON.stringify({ type: 'subscribed', deviceId: msg.deviceId }));
        } else if (msg.type === 'unsubscribe' && typeof msg.deviceId === 'string') {
          client.subscriptions.delete(msg.deviceId);
          ws.send(JSON.stringify({ type: 'unsubscribed', deviceId: msg.deviceId }));
        }
      } catch {
        // ignore malformed messages
      }
    });

    ws.on('close', () => {
      clients.delete(client);
      wsConnectionsActive.set(clients.size);
      logger.info({ total: clients.size }, 'WebSocket client disconnected');
    });

    // Send welcome message
    ws.send(JSON.stringify({ type: 'welcome', message: 'Connected to IoT platform' }));
  });

  // ── Redis Pub/Sub subscription ───────────────────────────────────
  const subClient = new Redis({
    host: config.REDIS_HOST,
    port: config.REDIS_PORT,
    maxRetriesPerRequest: 3,
  });

  await subClient.subscribe(RedisChannels.telemetryLive, RedisChannels.alertsLive);
  logger.info({ channels: [RedisChannels.telemetryLive, RedisChannels.alertsLive] }, 'Subscribed to Redis channels');

  subClient.on('message', (channel, message) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(message);
    } catch {
      return;
    }

    const envelope = JSON.stringify({
      type: channel === RedisChannels.alertsLive ? 'alert' : 'telemetry',
      data: parsed,
      receivedAt: new Date().toISOString(),
    });

    for (const client of clients) {
      if (client.ws.readyState !== WebSocket.OPEN) continue;

      // Filter: if client subscribed to specific devices, apply filter
      const deviceId = (parsed as { deviceId?: string }).deviceId;
      if (
        !client.subscriptions.has('*') &&
        (!deviceId || !client.subscriptions.has(deviceId))
      ) {
        continue;
      }

      // Backpressure: skip if the send buffer is too large
      if (client.ws.bufferedAmount > 1024 * 1024) {
        continue;
      }

      client.ws.send(envelope);
      wsMessagesSent.inc({ type: 'telemetry' });
    }
  });

  httpServer.listen(PORT, '0.0.0.0', () => {
    logger.info({ port: PORT }, 'api-websocket listening');
  });

  // ── Shutdown ─────────────────────────────────────────────────────
  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Shutting down api-websocket');
    for (const client of clients) client.ws.close();
    await subClient.quit();
    wss.close();
    httpServer.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});