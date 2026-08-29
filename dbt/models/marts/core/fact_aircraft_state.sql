{{ config(materialized='table') }}

-- FlightPulse: Phase 6 fact model
-- Ref: continuation doc section 7: "dim_aircraft / fact_aircraft_state"
--
-- Grain: one row per telemetry observation (same grain as
-- int_aircraft_activity / stg_opensky_states / raw_telemetry -- no
-- aggregation happens here). ingestion_id remains the natural key.
-- icao24 is the foreign key into dim_aircraft.

select
    ingestion_id,
    icao24,
    source,
    collector_id,

    callsign,
    origin_country,

    latitude,
    longitude,
    baro_altitude_m,
    geo_altitude_m,
    altitude_km,
    velocity_mps,
    velocity_kmh,
    true_track_deg,
    vertical_rate_mps,
    vertical_rate_category,
    on_ground,
    aircraft_activity_status,

    observation_date,
    observation_hour,
    telemetry_age_seconds,

    time_position_at,
    last_contact_at,
    received_at,
    ingested_at,
    processed_at

from {{ ref('int_aircraft_activity') }}
