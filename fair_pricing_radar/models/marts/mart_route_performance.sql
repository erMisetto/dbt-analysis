{{ config(materialized='table') }}

with route_parts as (
    select
        route,
        split_part(route, '-', 1) as origin,
        split_part(route, '-', 2) as destination
    from (
        select distinct route from {{ ref('mart_pricing_features') }}
    )
),

flagged_with_median as (
    -- Join pricing features with route daily prices to get median fare
    select
        f.route,
        f.any_rule_flag,
        f.total_fare,
        f.r1_price_outlier,
        f.r2_seat_scarcity,
        f.r3_monopoly_premium,
        f.carrier_code,
        rdp.median_fare
    from {{ ref('mart_pricing_features') }} f
    left join {{ ref('int_route_daily_prices') }} rdp
        on  rdp.route = f.route
        and rdp.carrier_code = f.carrier_code
        and rdp.dtf_bucket = f.dtf_bucket
        and rdp.flight_date = f.flight_date
)

select
    r.route,
    r.origin,
    r.destination,
    
    -- Add geo data
    og.lat_deg as origin_lat,
    og.lon_deg as origin_lon,
    dg.lat_deg as destination_lat,
    dg.lon_deg as destination_lon,
    
    -- Summary metrics
    count(*) as total_bookings,
    sum(f.any_rule_flag) as flagged_bookings,
    100.0 * sum(f.any_rule_flag) / count(*) as pct_flagged,
    
    -- Rule breakdown
    sum(f.r1_price_outlier) as r1_flags,
    sum(f.r2_seat_scarcity) as r2_flags,
    sum(f.r3_monopoly_premium) as r3_flags,
    
    -- Competition metrics
    rc.carrier_count,
    rc.hhi,
    case
        when rc.hhi >= 0.8 then 'High Concentration'
        when rc.hhi >= 0.5 then 'Moderate Concentration'
        else 'Low Concentration'
    end as concentration_level,
    
    -- Price metrics
    avg(f.total_fare) as avg_fare,
    median(f.total_fare) as median_fare,
    avg(case when f.any_rule_flag = 1 then f.total_fare else null end) as avg_flagged_fare,
    
    -- Distance metrics
    avg(f.trip_dist) as avg_distance_miles,
    
    -- Estimated extra cost - now using pre-joined median fare
    sum(case 
        when f.any_rule_flag = 1 and fm.median_fare is not null then 
            f.total_fare - fm.median_fare
        else 0 
    end) as total_extra_cost,
    
    -- Serving carriers list
    array_agg(distinct f.carrier_code) as serving_carriers

from route_parts r
join {{ ref('mart_pricing_features') }} f
    on r.route = f.route
left join {{ ref('seed_airport_geo') }} og
    on r.origin = og.iata_code
left join {{ ref('seed_airport_geo') }} dg
    on r.destination = dg.iata_code
left join {{ ref('int_route_competition') }} rc
    on r.route = rc.route
left join flagged_with_median fm
    on f.route = fm.route and f.carrier_code = fm.carrier_code
group by 
    r.route, 
    r.origin, 
    r.destination,
    og.lat_deg, 
    og.lon_deg, 
    dg.lat_deg, 
    dg.lon_deg,
    rc.carrier_count,
    rc.hhi