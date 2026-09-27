-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw telemetry hypertable
CREATE TABLE IF NOT EXISTS telemetry (
  time        TIMESTAMPTZ NOT NULL,
  device_id   TEXT NOT NULL,
  event_id    UUID NOT NULL,
  metrics     JSONB NOT NULL,
  tags        JSONB,
  source      TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable (partition by time, 1 day chunks)
SELECT create_hypertable('telemetry', 'time', if_not_exists => TRUE);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
  ON telemetry (device_id, time DESC);

-- Retention: auto-drop data older than 30 days
SELECT add_retention_policy('telemetry', INTERVAL '30 days', if_not_exists => TRUE);

-- Alerts table
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

-- Devices metadata table
CREATE TABLE IF NOT EXISTS devices (
  device_id   TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  model       TEXT,
  region      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen   TIMESTAMPTZ
);
