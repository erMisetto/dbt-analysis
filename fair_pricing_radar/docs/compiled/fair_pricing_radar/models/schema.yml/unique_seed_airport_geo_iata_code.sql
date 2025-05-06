
    
    

select
    iata_code as unique_field,
    count(*) as n_records

from FLIGHT_PRICES.WALRUS_PUBLIC.seed_airport_geo
where iata_code is not null
group by iata_code
having count(*) > 1


