{{ config(materialized='table') }}

-- FlightPulse: Phase 6 mart
-- Ref: continuation doc section 7: "mart_telemetry_quality: Missingness,
-- duplicates, invalid values and source-to-storage freshness."
-- Answers business questions (section 2) and KPIs (section 11):
--   - How fresh is telemetry from source observation to database availability?
--   - Quality: duplicate rate, invalid-record rate, dbt test pass rate (test
--     pass rate itself lives in dbt's own run results, not this mart --
--     see README note).
--
-- Grain: one row per (observation_date, observation_hour), combining
-- fact-table quality signals with extraction_log poll-cycle outcomes.
-- Two CTEs are unioned on date/hour because they come from different
-- grains upstream (fact = per-observation, extraction_log = per poll
-- cycle) and need a common time bucket to join on.

with fact as (

    select * from {{ ref('fact_aircraft_state') }}

),

fact_quality as (

    select
        observation_date,
        observation_hour,

        count(*)                                                      as total_observations,
        count(*) filter (where callsign is null)                      as missing_callsign_count,
        count(*) filter (where origin_country is null)                as missing_origin_country_count,
        count(*) filter (where latitude is null or longitude is null) as missing_position_count,
        count(*) filter (
            where latitude is not null
              and (latitude < -90 or latitude > 90)
        )                                                              as invalid_latitude_count,
        count(*) filter (
            where longitude is not null
              and (longitude < -180 or longitude > 180)
        )                                                              as invalid_longitude_count,
        count(*) filter (where velocity_kmh < 0)                       as invalid_velocity_count,

        avg(telemetry_age_seconds)                                     as avg_telemetry_age_seconds,
        max(telemetry_age_seconds)                                     as max_telemetry_age_seconds,
        percentile_cont(0.5) within group (order by telemetry_age_seconds) as p50_telemetry_age_seconds,
        percentile_cont(0.95) within group (order by telemetry_age_seconds) as p95_telemetry_age_seconds

    from fact
    group by observation_date, observation_hour

),

-- Duplicate source observations are prevented at the DB layer by
-- uq_raw_telemetry_ingestion_id, so genuine duplicates can't land in
-- raw_telemetry/fact_aircraft_state. What's measurable here instead is
-- collector-level *retry* duplication: the same (icao24, last_contact)
-- being seen more than once by ingestion_id, which the unique constraint
-- would have silently deduped at write time. We count it from the fact
-- table as a proxy since only successfully-persisted rows are visible
-- here; duplicates rejected at the DB layer aren't observable from dbt.
duplicate_check as (

    select
        observation_date,
        observation_hour,
        count(*)                                as row_count,
        count(distinct icao24 || '|' || last_contact_at::text) as distinct_observation_count
    from fact
    group by observation_date, observation_hour

),

extraction_quality as (

    select
        date(extraction_started_at)                          as observation_date,
        extract(hour from extraction_started_at)::int         as observation_hour,

        count(*)                                              as extraction_cycles,
        count(*) filter (where success is false)              as failed_extraction_cycles,
        sum(record_count)                                     as extraction_reported_record_count,
        avg(
            extract(epoch from (extraction_completed_at - extraction_started_at))
        )                                                      as avg_extraction_duration_seconds

    from {{ source('flightpulse', 'extraction_log') }}
    group by date(extraction_started_at), extract(hour from extraction_started_at)::int

)

select
    coalesce(fq.observation_date, eq.observation_date)   as observation_date,
    coalesce(fq.observation_hour, eq.observation_hour)   as observation_hour,

    fq.total_observations,
    fq.missing_callsign_count,
    fq.missing_origin_country_count,
    fq.missing_position_count,
    fq.invalid_latitude_count,
    fq.invalid_longitude_count,
    fq.invalid_velocity_count,

    dc.row_count - dc.distinct_observation_count         as retry_duplicate_count,

    fq.avg_telemetry_age_seconds,
    fq.max_telemetry_age_seconds,
    fq.p50_telemetry_age_seconds,
    fq.p95_telemetry_age_seconds,

    eq.extraction_cycles,
    eq.failed_extraction_cycles,
    eq.extraction_reported_record_count,
    eq.avg_extraction_duration_seconds

from fact_quality fq
full outer join extraction_quality eq
    on fq.observation_date = eq.observation_date
    and fq.observation_hour = eq.observation_hour
left join duplicate_check dc
    on fq.observation_date = dc.observation_date
    and fq.observation_hour = dc.observation_hour
