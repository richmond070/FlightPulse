-- FlightPulse data quality: coordinate bounds
-- Ref: continuation doc section 8: "Latitude and longitude remain within
-- valid geographic bounds."
-- A dbt test fails when this query returns any rows -- so we select the
-- violations.

select
    ingestion_id,
    icao24,
    latitude,
    longitude
from {{ ref('stg_opensky_states') }}
where
    (latitude is not null and (latitude < -90 or latitude > 90))
    or (longitude is not null and (longitude < -180 or longitude > 180))
