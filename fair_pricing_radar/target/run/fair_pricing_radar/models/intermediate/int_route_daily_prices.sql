
  
    

        create or replace transient table FLIGHT_PRICES.WALRUS_PUBLIC.int_route_daily_prices
         as
        (

with base as (

  select
    origin,
    destination,
    -- Define a route key so JFK-LAX and LAX-JFK stay distinct
    origin || '-' || destination        as route,

    /* bucket days_to_flight */
    case
      when days_to_flight between  0 and  3 then '00-03'
      when days_to_flight between  4 and  7 then '04-07'
      when days_to_flight between  8 and 29 then '08-29'
      else                                  '30+'
    end                                  as dtf_bucket,

    flight_date,
    search_date,

    -- cleaned two-letter carrier code (first element of the pipe-delimited string)
    trim(
      replace(
        split(segmentsairlinecode, '||')[0],
        '"',''
      )
    )                                    as carrier_code,

    total_fare,
    base_fare,
    total_fare / nullif(base_fare, 0)     as total_base_ratio

  from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches

),

agg as (

  select
    route,
    carrier_code              as marketing_carrier,
    dtf_bucket,
    flight_date,               -- keep flight_date for like-for-like comparisons
    count(*)                   as obs,
    median(total_fare)         as median_fare,
    percentile_cont(0.95)
      within group (order by total_fare)  as p95_fare,
    median(total_base_ratio)   as median_ratio,
    percentile_cont(0.95)
      within group (order by total_base_ratio) as p95_ratio

  from base
  group by
    route,
    carrier_code,
    dtf_bucket,
    flight_date

),

with_named as (

  select
    a.*,
    m.name as marketing_carrier_name
  from agg as a
  left join FLIGHT_PRICES.WALRUS_PUBLIC.carrier_mapping as m
    on a.marketing_carrier = m.code

)

select
  route,
  marketing_carrier        as carrier_code,
  marketing_carrier_name   as carrier_name,
  dtf_bucket,
  flight_date,
  obs,
  median_fare,
  p95_fare,
  median_ratio,
  p95_ratio
from with_named
        );
      
  