select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        vertical_rate_category as value_field,
        count(*) as n_records

    from "flightpulse"."public_intermediate"."int_aircraft_activity"
    group by vertical_rate_category

)

select *
from all_values
where value_field not in (
    'climbing','descending','stable','unknown'
)



      
    ) dbt_internal_test