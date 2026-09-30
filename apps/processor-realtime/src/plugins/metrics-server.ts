import { createServer, type Server } from 'node:http';
import { getMetricsRegistry } from '@iot/observability';
import { getLogger } from '@iot/observability';

let server: Server | undefined;

/**
 * Minimal HTTP server that exposes Prometheus metrics.
 * Kept separate from Fastify because the processor doesn't need a full web framework.
 */
export function startMetricsServer(port: number): Server {
  if (server) return server;

  const logger = getLogger();

  server = createServer((req, res) => {
    if (req.url === '/metrics') {
      const registry = getMetricsRegistry();
      registry
        .metrics()
        .then((body) => {
          res.writeHead(200, { 'Content-Type': registry.contentType });
          res.end(body);
        })
        .catch((err) => {
          res.writeHead(500);
          res.end(`error: ${err.message}`);
        });
      return;
    }

    if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', service: 'processor-realtime' }));
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(port, '0.0.0.0', () => {
    logger.info({ port }, 'processor metrics server listening');
  });

  return server;
}

export function stopMetricsServer(): void {
  server?.close();
  server = undefined;
}