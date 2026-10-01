-- AirBreda database schema (PostgreSQL)
-- Run once against your cloud database:
--   psql -h <DB_HOST> -U <DB_USER> -d airbreda -f schema.sql

-- sensor_readings: the table defined in the Day 1 lab (Lab 2), plus the Day 2 is_flagged column.
-- One row per (station, hour, component). We ingest component = 'NO2' only, but the design
-- allows PM10/O3 later without changing the table.
-- The PRIMARY KEY (station_id, timestamp, component) is what makes re-fetching safe:
-- Luchtmeetnet returns the last ~50 hours on every call, so the same reading arrives many
-- times. INSERT ... ON CONFLICT DO NOTHING skips the ones we already have (idempotent writes:
-- at-least-once fetching + idempotent writes, as the Day 1 "Exactly-Once Delivery" section advises).
CREATE TABLE IF NOT EXISTS sensor_readings (
    station_id   VARCHAR(20)  NOT NULL,                -- e.g. NL10240
    timestamp    TIMESTAMPTZ  NOT NULL,                -- end of the measured hour (UTC)
    component    VARCHAR(10)  NOT NULL,                -- 'NO2'
    value        FLOAT,                                -- µg/m³; NULL allowed: kept and flagged, never dropped
    is_flagged   BOOLEAN      NOT NULL DEFAULT FALSE,  -- Day 2: TRUE = stale or null reading
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, timestamp, component)
);

-- One row per run of an ingestion container.
-- Why this table exists: the ingestion containers are started by cron, do one fetch, and
-- exit. Anything kept "in memory" (like bad_data_count) disappears when the container exits.
-- The dashboard's /health endpoint needs last_successful_fetch and bad_data_count for BOTH
-- sources, so each run writes its result here, and /health reads it back.
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id              BIGSERIAL    PRIMARY KEY,
    source          TEXT         NOT NULL,   -- 'luchtmeetnet' or 'ndw'
    run_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    success         BOOLEAN      NOT NULL,
    rows_written    INTEGER      NOT NULL DEFAULT 0,
    bad_data_count  INTEGER      NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source_time
    ON ingestion_runs (source, run_at DESC);
