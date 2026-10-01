#!/bin/bash
echo "=== Docker services ==="
docker compose -f ~/iot-platform/infrastructure/compose/docker-compose.yml ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "=== Node.js services ==="
for port in 3001 3002 3003 3004 3005 3006 3007; do
  if ss -tlnp 2>/dev/null | grep -q ":$port "; then
    echo "  ✅ Port $port: UP"
  else
    echo "  ❌ Port $port: DOWN"
  fi
done

echo ""
echo "=== Port reference ==="
echo "  3001 = ingest-http"
echo "  3002 = processor-realtime (metrics)"
echo "  3003 = api-rest"
echo "  3004 = api-websocket"
echo "  3005 = processor-batch (metrics)"
echo "  3006 = archive-consumer (metrics)"
echo "  3007 = silver-transform (metrics)"
echo "  1883 = ingest-mqtt"
