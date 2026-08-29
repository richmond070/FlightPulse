{{ config(materialized='table') }}

-- FlightPulse: Phase 6 mart
-- Ref: continuation doc section 7: "mart_aircraft_activity: Aircraft
-- observations aggregated by time and aircraft."
-- Answers business questions (section 2):
--   - How many aircraft are observed during a given period?
--   - How does aircraft activity change by hour or day?
--   - Which aircraft/callsigns appear most frequently?
--   - What are the observed altitude and velocity patterns?
--   - How frequently are aircraft climbing, descending, or remaining stable?
--
-- Grain: one row per (icao24, observation_date, observation_hour).

with fact as (

    select * from {{ ref('fact_aircraft_state') }}

),

aggregated as (

    select
        icao24,
        observation_date,
        observation_hour,

        count(*)                                   as observation_count,

        -- most-frequent callsign in this window, not just "any" callsign
        mode() within group (order by callsign)     as most_frequent_callsign,

        avg(altitude_km)                            as avg_altitude_km,
        min(altitude_km)                            as min_altitude_km,
        max(altitude_km)                            as max_altitude_km,

        avg(velocity_kmh)                           as avg_velocity_kmh,
        min(velocity_kmh)                           as min_velocity_kmh,
        max(velocity_kmh)                           as max_velocity_kmh,

        count(*) filter (where aircraft_activity_status = 'climbing')     as climbing_count,
        count(*) filter (where aircraft_activity_status = 'descending')   as descending_count,
        count(*) filter (where aircraft_activity_status = 'level_flight') as level_flight_count,
        count(*) filter (where aircraft_activity_status = 'on_ground')    as on_ground_count,

        min(last_contact_at)                        as window_first_seen_at,
        max(last_contact_at)                        as window_last_seen_at

    from fact
    group by icao24, observation_date, observation_hour

)

select
    a.*,
    d.latest_callsign,
    d.latest_origin_country
from aggregated a
left join {{ ref('dim_aircraft') }} d using (icao24)
