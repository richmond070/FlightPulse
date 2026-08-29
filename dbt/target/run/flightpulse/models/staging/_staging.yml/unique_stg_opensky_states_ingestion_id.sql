select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    ingestion_id as unique_field,
    count(*) as n_records

from "flightpulse"."public_staging"."stg_opensky_states"
where ingestion_id is not null
group by ingestion_id
having count(*) > 1



      
    ) dbt_internal_test