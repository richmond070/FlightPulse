-- FlightPulse: promote collector_id to a real column
-- Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
-- section 3 (traceability of extraction metadata) and section 2's
-- business questions around system behavior/scaling, which need to be
-- answerable per-collector (e.g. "how does the platform behave when
-- traffic increases" implies being able to slice by which collector
-- instance produced a given row once multiple collectors run
-- concurrently, per the bounding-box test setup).
--
-- collector_id has been present in ingestion.schemas.TelemetryEvent and
-- therefore inside raw_telemetry.payload (JSONB) since it was added, but
-- was never promoted to its own column -- meaning it was validated and
-- stored, but not queryable/indexable without a JSONB extraction on
-- every query. This migration fixes that going forward and backfills
-- existing rows from the payload they already contain.

ALTER TABLE raw_telemetry
    ADD COLUMN IF NOT EXISTS collector_id TEXT;

-- Backfill from payload for any rows inserted before this column
-- existed, so existing data isn't orphaned from the new column.
UPDATE raw_telemetry
    SET collector_id = payload->>'collector_id'
    WHERE collector_id IS NULL
      AND payload->>'collector_id' IS NOT NULL;
