

-- FlightPulse: Phase 6 staging model
-- Ref: FlightPulse_Phase5_Continuation_ETL_Business_Objectives.pdf,
-- section 3 (Transform) and section 7 ("stg_opensky_states: Type casting,
-- standardized names/timestamps, null handling and source metadata.")
--
-- raw_telemetry keeps flight measurements inside `payload` JSONB by design
-- (see _sources.yml). This model is where they get typed out into real
-- columns for the first time. One row in = one row out; no aggregation,
-- no business logic here (that's int_aircraft_activity's job next).

with source as (

    select
        ingestion_id,
        source        as source_name,
        icao24        as icao24_raw,
        collector_id,
        received_at,
        ingested_at,
        processed_at,
        payload
    from "flightpulse"."public"."raw_telemetry"

),

cleaned as (

    select
        ingestion_id,
        source_name                                            as source,

        -- standardize identifier casing/whitespace -- OpenSky icao24 is
        -- normally already lowercase hex, but don't assume upstream never
        -- changes.
        lower(trim(icao24_raw))                                as icao24,

        collector_id,
        nullif(trim(payload ->> 'callsign'), '')               as callsign,
        nullif(trim(payload ->> 'origin_country'), '')         as origin_country,

        -- OpenSky epoch-second fields -> real timestamps. Null-safe: an
        -- empty/invalid epoch casts to null rather than erroring the model.
        to_timestamp(nullif(payload ->> 'time_position', '')::bigint)  as time_position_at,
        to_timestamp(nullif(payload ->> 'last_contact', '')::bigint)   as last_contact_at,

        (payload ->> 'longitude')::double precision            as longitude,
        (payload ->> 'latitude')::double precision             as latitude,
        (payload ->> 'baro_altitude_m')::double precision      as baro_altitude_m,
        (payload ->> 'geo_altitude_m')::double precision       as geo_altitude_m,
        (payload ->> 'velocity_mps')::double precision         as velocity_mps,
        (payload ->> 'true_track_deg')::double precision       as true_track_deg,
        (payload ->> 'vertical_rate_mps')::double precision    as vertical_rate_mps,
        (payload ->> 'on_ground')::boolean                     as on_ground,

        received_at,
        ingested_at,
        processed_at

    from source

)

select * from cleaned