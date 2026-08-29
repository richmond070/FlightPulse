select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- FlightPulse data quality: ICAO24 format
-- Ref: continuation doc section 8: "ICAO24 follows the expected identifier
-- format where present." OpenSky icao24 is a 6-character lowercase hex
-- string. stg_opensky_states already lower()/trim()s it, so a failure here
-- means genuinely malformed source data, not a casing issue.

select
    ingestion_id,
    icao24
from "flightpulse"."public_staging"."stg_opensky_states"
where icao24 is not null
  and icao24 !~ '^[0-9a-f]{6}$'
      
    ) dbt_internal_test