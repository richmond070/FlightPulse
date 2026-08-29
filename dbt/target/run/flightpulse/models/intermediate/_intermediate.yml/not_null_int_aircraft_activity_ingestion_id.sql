select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ingestion_id
from "flightpulse"."public_intermediate"."int_aircraft_activity"
where ingestion_id is null



      
    ) dbt_internal_test