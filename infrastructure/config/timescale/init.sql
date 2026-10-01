-- ─────────────────────────────────────────────────────────────────────
-- TimescaleDB schema for IoT platform
-- ─────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─────────────────────────────────────────────────────────────────────
-- Raw telemetry hypertable
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS telemetry (
  time        TIMESTAMPTZ NOT NULL,
  device_id   TEXT NOT NULL,
  event_id    UUID NOT NULL,
  metrics     JSONB NOT NULL,
  tags        JSONB,
  source      TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable (auto-partition by time, 1-day chunks)
SELECT create_hypertable(
  'telemetry',
  'time',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
  ON telemetry (device_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_event_id
  ON telemetry (event_id);

-- Retention: auto-drop data older than 30 days
SELECT add_retention_policy('telemetry', INTERVAL '30 days', if_not_exists => TRUE);

-- ─────────────────────────────────────────────────────────────────────
-- Continuous aggregate: 1-minute rollups
-- ─────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', time) AS bucket,
  device_id,
  -- Compute averages for common metrics using JSONB extraction
  AVG((metrics->>'temperature')::numeric) AS avg_temperature,
  MIN((metrics->>'temperature')::numeric) AS min_temperature,
  MAX((metrics->>'temperature')::numeric) AS max_temperature,
  AVG((metrics->>'humidity')::numeric) AS avg_humidity,
  MIN((metrics->>'humidity')::numeric) AS min_humidity,
  MAX((metrics->>'humidity')::numeric) AS max_humidity,
  AVG((metrics->>'pressure')::numeric) AS avg_pressure,
  COUNT(*) AS sample_count
FROM telemetry
GROUP BY bucket, device_id
WITH NO DATA;

-- Refresh policy: refresh continuous aggregate for data between 3 hours ago and now
SELECT add_continuous_aggregate_policy('telemetry_1m',
  start_offset => INTERVAL '3 hours',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute',
  if_not_exists => TRUE
);

-- ─────────────────────────────────────────────────────────────────────
-- Continuous aggregate: 5-minute rollups (from 1m, cheaper)
-- ─────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_5m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('5 minutes', bucket) AS bucket,
  device_id,
  AVG(avg_temperature) AS avg_temperature,
  MIN(min_temperature) AS min_temperature,
  MAX(max_temperature) AS max_temperature,
  AVG(avg_humidity) AS avg_humidity,
  SUM(sample_count) AS sample_count
FROM telemetry_1m
GROUP BY time_bucket('5 minutes', bucket), device_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('telemetry_5m',
  start_offset => INTERVAL '1 day',
  end_offset   => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes',
  if_not_exists => TRUE
);

-- ─────────────────────────────────────────────────────────────────────
-- Continuous aggregate: 1-hour rollups
-- ─────────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', bucket) AS bucket,
  device_id,
  AVG(avg_temperature) AS avg_temperature,
  MIN(min_temperature) AS min_temperature,
  MAX(max_temperature) AS max_temperature,
  AVG(avg_humidity) AS avg_humidity,
  SUM(sample_count) AS sample_count
FROM telemetry_1m
GROUP BY time_bucket('1 hour', bucket), device_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('telemetry_1h',
  start_offset => INTERVAL '7 days',
  end_offset   => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour',
  if_not_exists => TRUE
);

-- ─────────────────────────────────────────────────────────────────────
-- Alerts table (regular, not hypertable — low volume)
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
  alert_id     UUID PRIMARY KEY,
  device_id    TEXT NOT NULL,
  rule_id      TEXT NOT NULL,
  severity     TEXT NOT NULL,
  message      TEXT NOT NULL,
  value        DOUBLE PRECISION,
  threshold    DOUBLE PRECISION,
  triggered_at TIMESTAMPTZ NOT NULL,
  acknowledged BOOLEAN DEFAULT FALSE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_device_time
  ON alerts (device_id, triggered_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_severity_open
  ON alerts (severity, triggered_at DESC)
  WHERE acknowledged = FALSE;

-- ─────────────────────────────────────────────────────────────────────
-- Devices metadata (regular table)
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS devices (
  device_id   TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  model       TEXT,
  region      TEXT,
  first_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen   TIMESTAMPTZ
);