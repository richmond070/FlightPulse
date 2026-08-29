{{ config(materialized='view') }}

-- FlightPulse: Phase 6 intermediate model
-- Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
-- section 7: "int_aircraft_activity: Derives altitude_km, velocity_kmh,
-- vertical_rate_category, observation_date/observation_hour,
-- aircraft_activity_status, telemetry_age_seconds."
--
-- One row in from stg_opensky_states, one row out. No aggregation --
-- that's the marts' job. Every derived field here maps to a specific
-- line in the continuation doc so this stays traceable back to spec.

with staged as (

    select * from {{ ref('stg_opensky_states') }}

),

derived as (

    select
        ingestion_id,
        source,
        icao24,
        callsign,
        origin_country,
        collector_id,

        latitude,
        longitude,
        baro_altitude_m,
        geo_altitude_m,
        velocity_mps,
        true_track_deg,
        vertical_rate_mps,
        on_ground,

        -- Prefer geometric altitude (GNSS-derived) over barometric when
        -- both are present, since geo_altitude_m isn't affected by local
        -- pressure calibration. Falls back to baro when geo is null.
        round((coalesce(geo_altitude_m, baro_altitude_m) / 1000.0)::numeric, 3)
            as altitude_km,

        round((velocity_mps * 3.6)::numeric, 2)
            as velocity_kmh,

        -- Thresholds: OpenSky vertical_rate is metres/second. +/-1 m/s
        -- (~200 ft/min) is the conventional noise floor below which GPS/
        -- barometric jitter looks like climb/descent when the aircraft is
        -- actually level.
        case
            when vertical_rate_mps is null then 'unknown'
            when vertical_rate_mps > 1     then 'climbing'
            when vertical_rate_mps < -1    then 'descending'
            else 'stable'
        end as vertical_rate_category,

        -- Combines on_ground with vertical rate into a single status field
        -- so marts don't each have to re-derive it from two columns.
        case
            when on_ground is true                    then 'on_ground'
            when on_ground is null                     then 'unknown'
            when vertical_rate_mps is null             then 'airborne_unknown_rate'
            when vertical_rate_mps > 1                 then 'climbing'
            when vertical_rate_mps < -1                then 'descending'
            else 'level_flight'
        end as aircraft_activity_status,

        date(last_contact_at)      as observation_date,
        extract(hour from last_contact_at)::int as observation_hour,

        -- End-to-end pipeline latency: source observation time -> the
        -- moment worker/persistence.py actually wrote the row (processed_at
        -- = now() at INSERT, per worker/persistence.py). This is the
        -- freshness KPI mart_telemetry_quality reports on.
        extract(epoch from (processed_at - last_contact_at))::numeric
            as telemetry_age_seconds,

        time_position_at,
        last_contact_at,
        received_at,
        ingested_at,
        processed_at

    from staged

)

select * from derived
