
  
    

        create or replace transient table FLIGHT_PRICES.WALRUS_PUBLIC.dim_carrier
         as
        (

select
  code   as carrier_code,
  name   as carrier_name
from FLIGHT_PRICES.WALRUS_PUBLIC.carrier_mapping
        );
      
  