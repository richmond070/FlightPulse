-- FlightPulse data quality: telemetry latency sanity check
-- Ref: continuation doc section 7/8 -- telemetry_age_seconds feeds the
-- freshness KPI in mart_telemetry_quality. A negative value would mean
-- processed_at < last_contact_at, i.e. the row was persisted before the
-- source claims it was observed.
--
-- Configured as warn (not error) in dbt_project.yml-level severity below,
-- since minor clock skew across the collector/OpenSky/DB is a plausible
-- real-world cause and shouldn't hard-fail the whole test suite.

{{ config(severity='warn') }}

select
    ingestion_id,
    icao24,
    last_contact_at,
    processed_at,
    telemetry_age_seconds
from {{ ref('int_aircraft_activity') }}
where telemetry_age_seconds < 0
