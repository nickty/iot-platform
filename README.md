# IoT Data Platform

A data-intensive IoT platform for practicing senior-level software engineering:
streaming ingestion, real-time processing, batch analytics, multi-cloud deployment.

## Stack

- **Runtime**: Node.js 22, TypeScript
- **Ingestion**: Fastify (HTTP), Aedes (MQTT)
- **Broker**: Redpanda (Kafka-compatible)
- **Storage**: TimescaleDB, Redis, MinIO (S3-compatible)
- **Observability**: Prometheus, Grafana, Jaeger, Loki, OpenTelemetry
- **Orchestration**: Docker Compose (local), Terraform (cloud)

## Quick Start

```bash
# Start infrastructure
cd infrastructure/compose
docker compose up -d

# Verify all services
docker compose ps

# Open UIs
# Redpanda Console: http://localhost:8080
# Grafana:          http://localhost:3000 (admin/admin)
# Jaeger:           http://localhost:16686
# MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)
# Prometheus:       http://localhost:9090
