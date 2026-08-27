-- FlightPulse: Phase 5 persistence schema addition
-- Ref: FlightPulse_Complete_Project_Workflow_Guide.pdf, section 9, Phase 5
-- ("Record ingestion and processing timestamps.") and Phase 7
-- ("Measure requests/sec, queue depth, latency and DB write throughput.")
--
-- received_at (row insert time) and ingested_at (collector-assigned event
-- time) already exist from Phase 1. processed_at captures the moment the
-- worker actually wrote the row, distinct from received_at's DEFAULT now()
-- -- this lets Phase 7 measure queue-to-persisted latency (processed_at
-- minus ingested_at) without retrofitting later.

ALTER TABLE raw_telemetry
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;
