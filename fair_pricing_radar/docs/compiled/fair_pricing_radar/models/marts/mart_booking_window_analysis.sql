

select
    dtf_bucket,
    fare_bucket,
    
    -- Count metrics
    count(*) as total_bookings,
    sum(any_rule_flag) as flagged_bookings,
    100.0 * sum(any_rule_flag) / nullif(count(*), 0) as pct_flagged,
    
    -- Rule breakdown
    sum(r1_price_outlier) as r1_flags,
    sum(r2_seat_scarcity) as r2_flags,
    sum(r3_monopoly_premium) as r3_flags,
    
    -- Fare metrics by bucket
    avg(total_fare) as avg_fare,
    median(total_fare) as median_fare,
    stddev(total_fare) as stddev_fare,
    min(total_fare) as min_fare,
    max(total_fare) as max_fare,
    
    -- Flagged fares metrics
    avg(case when any_rule_flag = 1 then total_fare else null end) as avg_flagged_fare,
    median(case when any_rule_flag = 1 then total_fare else null end) as median_flagged_fare,
    
    -- Premium calculation (average difference between flagged and normal)
    avg(case when any_rule_flag = 1 then total_fare else null end) - 
    avg(case when any_rule_flag = 0 then total_fare else null end) as avg_premium,
    
    -- Booking patterns
    count(distinct carrier_code) as unique_carriers,
    count(distinct route) as unique_routes

from FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features
group by dtf_bucket, fare_bucket
order by 
    case 
        when dtf_bucket = '00-03' then 1
        when dtf_bucket = '04-07' then 2
        when dtf_bucket = '08-29' then 3
        when dtf_bucket = '30+' then 4
    end,
    fare_bucket