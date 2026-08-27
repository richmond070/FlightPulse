-- FlightPulse: extraction log
-- Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
-- section 3, Extract: "It should handle authentication where required,
-- respect current API limits, request only the required scope, preserve
-- source timestamps, and record extraction failures."
--
-- Fields called out explicitly: source, extraction_started_at,
-- extraction_completed_at, source_observation_time, request_id,
-- collector_version, record_count, request_scope.
--
-- One row per collector poll cycle (not per event) -- this is metadata
-- about the *extraction request*, distinct from raw_telemetry which
-- holds the actual observations. Kept as its own table rather than
-- columns bolted onto raw_telemetry since the cardinality and query
-- patterns are different (one extraction_log row summarizes N
-- raw_telemetry rows).

CREATE TABLE IF NOT EXISTS extraction_log (
    id                      BIGSERIAL PRIMARY KEY,
    request_id              UUID NOT NULL UNIQUE,
    source                  TEXT NOT NULL,
    collector_id            TEXT,
    collector_version       TEXT,
    request_scope           TEXT,          -- e.g. bbox tuple as text, or 'global'
    extraction_started_at   TIMESTAMPTZ NOT NULL,
    extraction_completed_at TIMESTAMPTZ,
    source_observation_time TIMESTAMPTZ,   -- OpenSky's own 'time' field for this response
    record_count            INTEGER,
    success                 BOOLEAN NOT NULL,
    error_message           TEXT,
    logged_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports "how fresh is telemetry" and "how does the platform behave
-- under load" business questions (section 2) by making recent extraction
-- history queryable by time.
CREATE INDEX IF NOT EXISTS ix_extraction_log_started_at
    ON extraction_log (extraction_started_at DESC);
