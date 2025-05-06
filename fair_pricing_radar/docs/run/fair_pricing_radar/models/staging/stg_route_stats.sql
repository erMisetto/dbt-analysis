
  
    

        create or replace transient table FLIGHT_PRICES.WALRUS_PUBLIC.stg_route_stats
         as
        (

with base as (

  select
    route,
    fare_bucket,
    usd_per_mi
  from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches
  where usd_per_mi is not null

),

bench as (

  select
    route,
    fare_bucket,

    -- Percentile benchmarks (unqualified usd_per_mi)
    percentile_cont(0.50) within group (order by usd_per_mi) as p50_usd_per_mi,
    percentile_cont(0.90) within group (order by usd_per_mi) as p90_usd_per_mi,
    percentile_cont(0.95) within group (order by usd_per_mi) as p95_usd_per_mi,
    percentile_cont(0.97) within group (order by usd_per_mi) as p97_usd_per_mi,

    max(usd_per_mi)   as max_usd_per_mi,
    count(*)          as legs_sampled

  from base
  group by
    route,
    fare_bucket

)

select * from bench
        );
      
  