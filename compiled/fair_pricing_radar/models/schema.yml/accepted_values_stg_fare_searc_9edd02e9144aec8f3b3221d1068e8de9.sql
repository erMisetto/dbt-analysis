
    
    

with all_values as (

    select
        seats_flag as value_field,
        count(*) as n_records

    from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches
    group by seats_flag

)

select *
from all_values
where value_field not in (
    'sold_out','scarce','9plus'
)


