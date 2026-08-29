select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select icao24
from "flightpulse"."public_staging"."stg_opensky_states"
where icao24 is null



      
    ) dbt_internal_test