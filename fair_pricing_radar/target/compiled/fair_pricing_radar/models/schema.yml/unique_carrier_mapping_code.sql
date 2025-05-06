
    
    

select
    code as unique_field,
    count(*) as n_records

from FLIGHT_PRICES.WALRUS_PUBLIC.carrier_mapping
where code is not null
group by code
having count(*) > 1


