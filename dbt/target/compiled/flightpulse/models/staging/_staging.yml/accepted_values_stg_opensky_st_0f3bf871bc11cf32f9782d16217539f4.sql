
    
    

with all_values as (

    select
        on_ground as value_field,
        count(*) as n_records

    from (select * from "flightpulse"."public_staging"."stg_opensky_states" where on_ground is not null) dbt_subquery
    group by on_ground

)

select *
from all_values
where value_field not in (
    True,False
)


