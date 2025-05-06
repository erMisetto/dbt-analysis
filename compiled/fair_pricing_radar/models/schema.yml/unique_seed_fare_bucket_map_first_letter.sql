
    
    

select
    first_letter as unique_field,
    count(*) as n_records

from FLIGHT_PRICES.WALRUS_PUBLIC.seed_fare_bucket_map
where first_letter is not null
group by first_letter
having count(*) > 1


