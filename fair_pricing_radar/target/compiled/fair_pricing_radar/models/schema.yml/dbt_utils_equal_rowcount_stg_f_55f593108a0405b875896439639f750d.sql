

with a as (

    select count(*) as count_a from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches

),
b as (

    select count(*) as count_b from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches

),
final as (

    select
        count_a,
        count_b,
        abs(count_a - count_b) as diff_count
    from a
    cross join b

)

select * from final

