
  
    

        create or replace transient table FLIGHT_PRICES.WALRUS_PUBLIC.int_flagged_extra_costs
         as
        (

-- 1. Grab only the bookings we’ve flagged as suspicious
with flagged as (

  select
    legid,
    route,
    carrier_code            as marketing_carrier,
    dtf_bucket,
    flight_date,
    total_fare,
    r1_price_outlier        as is_r1,
    r2_seat_scarcity        as is_r2,
    r3_monopoly_premium     as is_r3

  from FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features
  where any_rule_flag = 1

),

-- 2. Pull in the median benchmark (use carrier_code → marketing_carrier)
bench as (

  select
    route,
    carrier_code            as marketing_carrier,
    dtf_bucket,
    flight_date,
    median_fare

  from FLIGHT_PRICES.WALRUS_PUBLIC.int_route_daily_prices

),

-- 3. Compute overage per booking
extra as (

  select
    f.*,
    b.median_fare,
    case
      when f.total_fare > b.median_fare then
        f.total_fare - b.median_fare
      else 0
    end as extra_cost

  from flagged f
  left join bench b
    using (route, marketing_carrier, dtf_bucket, flight_date)

)

select * from extra
        );
      
  