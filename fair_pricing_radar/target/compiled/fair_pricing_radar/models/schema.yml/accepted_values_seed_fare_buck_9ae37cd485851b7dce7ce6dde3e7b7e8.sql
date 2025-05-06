
    
    

with all_values as (

    select
        fare_bucket as value_field,
        count(*) as n_records

    from FLIGHT_PRICES.WALRUS_PUBLIC.seed_fare_bucket_map
    group by fare_bucket

)

select *
from all_values
where value_field not in (
    'prem_cabin','full_mid_econ','deep_disc_econ'
)


