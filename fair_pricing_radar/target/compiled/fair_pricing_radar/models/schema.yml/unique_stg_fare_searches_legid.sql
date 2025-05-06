
    
    

select
    legid as unique_field,
    count(*) as n_records

from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches
where legid is not null
group by legid
having count(*) > 1


