{{ config(materialized='table') }}

-- FlightPulse: Phase 6 dimension model
-- Ref: continuation doc section 7: "dim_aircraft / fact_aircraft_state"
--
-- Important design note: there is no external aircraft registry feeding
-- this pipeline (no tail-number/manufacturer master data source exists
-- anywhere upstream). callsign and origin_country are observed telemetry
-- fields, not static attributes -- a callsign changes per flight leg. So
-- this dimension is built as an aggregate over int_aircraft_activity:
-- "most recently observed" callsign/origin_country, plus first/last-seen
-- bounds and an observation count. If a real registry is ever sourced,
-- this model is where it would be joined in.

with activity as (

    select * from {{ ref('int_aircraft_activity') }}

),

aggregated as (

    select
        icao24,
        min(last_contact_at)       as first_seen_at,
        max(last_contact_at)       as last_seen_at,
        count(*)                   as observation_count,
        count(distinct source)     as source_count
    from activity
    group by icao24

),

-- One row per icao24: the callsign/origin_country from that aircraft's
-- single most recent observation (by last_contact_at).
most_recent as (

    select distinct on (icao24)
        icao24,
        callsign        as latest_callsign,
        origin_country  as latest_origin_country
    from activity
    order by icao24, last_contact_at desc nulls last

)

select
    a.icao24,
    m.latest_callsign,
    m.latest_origin_country,
    a.first_seen_at,
    a.last_seen_at,
    a.observation_count,
    a.source_count
from aggregated a
left join most_recent m using (icao24)
