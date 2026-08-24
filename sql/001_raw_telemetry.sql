-- FlightPulse: Phase 1 foundation schema
-- Layer: raw_telemetry — immutable-ish source landing table
-- Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 5 (PostgreSQL Data Model)

CREATE TABLE IF NOT EXISTS raw_telemetry (
    id              BIGSERIAL PRIMARY KEY,
    ingestion_id    UUID NOT NULL,
    source          TEXT NOT NULL DEFAULT 'opensky',
    icao24          TEXT NOT NULL,
    payload         JSONB NOT NULL,       -- full normalized event, retained for traceability
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingested_at     TIMESTAMPTZ NOT NULL
);

-- Index ICAO24 + event time together for aircraft-history queries.
CREATE INDEX IF NOT EXISTS idx_raw_telemetry_icao24_ingested_at
    ON raw_telemetry (icao24, ingested_at);

-- Deduplication strategy based on source identifiers + timestamps.
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_telemetry_ingestion_id
    ON raw_telemetry (ingestion_id);
