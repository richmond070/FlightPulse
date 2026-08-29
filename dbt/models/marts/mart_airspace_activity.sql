{{ config(materialized='table') }}

-- FlightPulse: Phase 6 mart
-- Ref: continuation doc section 7: "mart_airspace_activity: Aircraft
-- density/activity aggregated by geographic area and time."
-- Answers business questions (section 2):
--   - Which geographic regions have the highest observed aircraft density?
--   - How does airspace activity vary geographically and temporally?
--
-- "Geographic area" is approximated with a 1-degree lat/lon grid cell,
-- since there's no administrative-boundary/airspace-sector reference data
-- in this pipeline (only raw lat/lon from OpenSky). 1 degree is roughly
-- 111km at the equator -- coarse enough to aggregate meaningfully, fine
-- enough to show regional variation. Rows with a null lat/lon (fully
-- on-ground/no-position-fix observations) are excluded, since they can't
-- be placed in a cell.
--
-- Grain: one row per (lat_grid, lon_grid, observation_date, observation_hour).

with fact as (

    select *
    from {{ ref('fact_aircraft_state') }}
    where latitude is not null
      and longitude is not null

),

gridded as (

    select
        floor(latitude)::int   as lat_grid,
        floor(longitude)::int  as lon_grid,
        observation_date,
        observation_hour,
        icao24,
        altitude_km,
        velocity_kmh,
        aircraft_activity_status
    from fact

),

aggregated as (

    select
        lat_grid,
        lon_grid,
        observation_date,
        observation_hour,

        count(*)                       as observation_count,
        count(distinct icao24)         as distinct_aircraft_count,

        avg(altitude_km)               as avg_altitude_km,
        avg(velocity_kmh)              as avg_velocity_kmh,

        count(*) filter (where aircraft_activity_status = 'climbing')     as climbing_count,
        count(*) filter (where aircraft_activity_status = 'descending')   as descending_count,
        count(*) filter (where aircraft_activity_status = 'level_flight') as level_flight_count

    from gridded
    group by lat_grid, lon_grid, observation_date, observation_hour

)

select
    *,
    -- cell center, handy for mapping/plotting without re-deriving from the grid ints
    lat_grid + 0.5 as lat_grid_center,
    lon_grid + 0.5 as lon_grid_center
from aggregated
