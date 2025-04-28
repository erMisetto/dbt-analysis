{{ config(materialized='table') }}

with flagged_with_median as (
    -- Join pricing features with route daily prices to get median fare
    select
        f.carrier_code,
        f.any_rule_flag,
        f.total_fare,
        f.r1_price_outlier,
        f.r2_seat_scarcity,
        f.r3_monopoly_premium,
        f.route,
        rdp.median_fare
    from {{ ref('mart_pricing_features') }} f
    left join {{ ref('int_route_daily_prices') }} rdp
        on  rdp.route = f.route
        and rdp.carrier_code = f.carrier_code
        and rdp.dtf_bucket = f.dtf_bucket
        and rdp.flight_date = f.flight_date
),

route_competition_check as (
    -- Get route competition data
    select
        f.route,
        f.carrier_code,
        rc.hhi
    from {{ ref('mart_pricing_features') }} f
    left join {{ ref('int_route_competition') }} rc
        on rc.route = f.route
    group by f.route, f.carrier_code, rc.hhi
)

select
    c.carrier_code,
    c.carrier_name,
    
    -- Booking volumes
    count(*) as total_bookings,
    sum(f.any_rule_flag) as flagged_bookings,
    100.0 * sum(f.any_rule_flag) / nullif(count(*), 0) as pct_flagged,
    
    -- Flag type breakdown
    sum(f.r1_price_outlier) as r1_price_outlier_count,
    sum(f.r2_seat_scarcity) as r2_seat_scarcity_count,
    sum(f.r3_monopoly_premium) as r3_monopoly_premium_count,
    
    -- Percentage breakdown
    100.0 * sum(f.r1_price_outlier) / nullif(count(*), 0) as pct_r1_outliers,
    100.0 * sum(f.r2_seat_scarcity) / nullif(count(*), 0) as pct_r2_scarcity,
    100.0 * sum(f.r3_monopoly_premium) / nullif(count(*), 0) as pct_r3_monopoly,
    
    -- Fare metrics
    avg(f.total_fare) as avg_fare,
    median(f.total_fare) as median_fare,
    avg(case when f.any_rule_flag = 1 then f.total_fare else null end) as avg_flagged_fare,
    
    -- Extra cost total - now using pre-joined median fare
    sum(case 
        when f.any_rule_flag = 1 and fm.median_fare is not null then 
            f.total_fare - fm.median_fare
        else 0 
    end) as total_extra_cost,
    
    -- Number of routes served
    count(distinct f.route) as routes_served,
    
    -- Monopoly routes count
    sum(case
        when rc.hhi >= {{ var('r3_hhi_threshold', 0.80) }} then 1
        else 0
    end) as monopoly_routes_count

from {{ ref('mart_pricing_features') }} f
join {{ ref('dim_carrier') }} c
    on f.carrier_code = c.carrier_code
left join flagged_with_median fm
    on f.carrier_code = fm.carrier_code and f.route = fm.route
left join route_competition_check rc
    on f.carrier_code = rc.carrier_code and f.route = rc.route
group by c.carrier_code, c.carrier_name