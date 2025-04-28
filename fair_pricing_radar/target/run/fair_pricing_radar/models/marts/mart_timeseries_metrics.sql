
  
    

        create or replace transient table FLIGHT_PRICES.WALRUS_PUBLIC.mart_timeseries_metrics
         as
        (

with flagged_with_median as (
    -- Join pricing features with route daily prices to get median fare
    select
        mpf.flight_date,
        mpf.any_rule_flag,
        mpf.total_fare,
        mpf.r1_price_outlier,
        mpf.r2_seat_scarcity,
        mpf.r3_monopoly_premium,
        rdp.median_fare
    from FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features mpf
    left join FLIGHT_PRICES.WALRUS_PUBLIC.int_route_daily_prices rdp
        on  rdp.route = mpf.route
        and rdp.carrier_code = mpf.carrier_code
        and rdp.dtf_bucket = mpf.dtf_bucket
        and rdp.flight_date = mpf.flight_date
)

select
    flight_date,
    count(*) as total_bookings,
    sum(any_rule_flag) as flagged_bookings,
    100.0 * sum(any_rule_flag) / count(*) as pct_flagged,
    
    -- Average prices
    avg(total_fare) as avg_total_fare,
    median(total_fare) as median_total_fare,
    avg(case when any_rule_flag = 1 then total_fare else null end) as avg_flagged_fare,
    
    -- Rule breakdown
    sum(r1_price_outlier) as r1_flags,
    sum(r2_seat_scarcity) as r2_flags,
    sum(r3_monopoly_premium) as r3_flags,
    
    -- Extra cost calculation - now using pre-joined median fare
    sum(case 
        when any_rule_flag = 1 and median_fare is not null then 
            total_fare - median_fare
        else 0 
    end) as daily_extra_cost

from flagged_with_median
group by flight_date
order by flight_date
        );
      
  