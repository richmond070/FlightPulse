
    
    

with all_values as (

    select
        aircraft_activity_status as value_field,
        count(*) as n_records

    from "flightpulse"."public_intermediate"."int_aircraft_activity"
    group by aircraft_activity_status

)

select *
from all_values
where value_field not in (
    'on_ground','climbing','descending','level_flight','airborne_unknown_rate','unknown'
)


