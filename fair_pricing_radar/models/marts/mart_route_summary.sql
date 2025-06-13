{{ config(materialized='table') }}

-- Complete route coverage for competition analysis (Visualizations 5 & 6)
-- Supports: "Competition Breakdown" scatter plot and "Monopoly Alert" bubble chart

with route_base as (
    select
        route,
        split_part(route, '-', 1) as origin,
        split_part(route, '-', 2) as destination,
        carrier_code,
        total_fare,
        trip_dist,
        any_rule_flag,
        r1_price_outlier,
        r2_seat_scarcity,
        r3_monopoly_premium,
        hhi,
        carrier_count
    from {{ ref('mart_pricing_features') }}
),

-- Route-level aggregations
route_metrics as (
    select
        route,
        origin,
        destination,
        
        -- Volume metrics
        count(*) as total_bookings,
        sum(any_rule_flag) as flagged_bookings,
        count(distinct carrier_code) as unique_carriers_actual,
        
        -- Alert rates (critical for scatter plots)
        round(100.0 * sum(any_rule_flag) / count(*), 2) as route_alert_rate,
        round(100.0 * sum(r1_price_outlier) / count(*), 2) as r1_rate,
        round(100.0 * sum(r2_seat_scarcity) / count(*), 2) as r2_rate,  
        round(100.0 * sum(r3_monopoly_premium) / count(*), 2) as r3_rate,
        
        -- Competition metrics (from individual records)
        avg(hhi) as hhi,
        avg(carrier_count) as carrier_count,
        
        -- Financial metrics
        avg(total_fare) as avg_fare,
        median(total_fare) as median_fare,
        avg(case when any_rule_flag = 1 then total_fare else null end) as avg_flagged_fare,
        sum(total_fare) as total_revenue,
        
        -- Distance metrics
        avg(trip_dist) as avg_distance_miles,
        
        -- Serving carriers (for route analysis)
        listagg(distinct carrier_code, ',') within group (order by carrier_code) as serving_carriers
        
    from route_base
    group by route, origin, destination
),

-- Add geographic coordinates for map visualizations
route_geo as (
    select
        rm.*,
        
        -- Origin coordinates
        og.lat_deg as origin_lat,
        og.lon_deg as origin_lon,
        
        -- Destination coordinates  
        dg.lat_deg as destination_lat,
        dg.lon_deg as destination_lon
        
    from route_metrics rm
    left join {{ ref('seed_airport_geo') }} og
        on rm.origin = og.iata_code
    left join {{ ref('seed_airport_geo') }} dg  
        on rm.destination = dg.iata_code
),

-- Calculate extra costs using route-level medians
route_extra_costs as (
    select
        rb.route,
        sum(case 
            when rb.any_rule_flag = 1 then 
                rb.total_fare - percentile_cont(0.5) within group (order by rb.total_fare) over (partition by rb.route)
            else 0 
        end) as total_extra_cost
    from route_base rb
    group by rb.route
),

-- Final enhanced route summary
final_summary as (
    select
        rg.*,
        coalesce(rec.total_extra_cost, 0) as total_extra_cost,
        
        -- Pre-calculated fields for Tableau performance
        case 
            when rg.hhi >= 0.8 then 'High Concentration'
            when rg.hhi >= 0.5 then 'Moderate Concentration'
            else 'Low Concentration'
        end as concentration_level,
        
        -- Monopoly flags for filtering
        case when rg.hhi >= 0.8 then 1 else 0 end as is_monopoly_route,
        case when rg.carrier_count = 1 then 1 else 0 end as is_single_carrier,
        
        -- Volume categories for bubble sizing
        case 
            when rg.total_bookings >= 10000 then 'High Volume'
            when rg.total_bookings >= 1000 then 'Medium Volume'
            else 'Low Volume'
        end as volume_category,
        
        -- Alert rate categories
        case 
            when rg.route_alert_rate >= 30 then 'High Alert'
            when rg.route_alert_rate >= 15 then 'Medium Alert'
            else 'Low Alert'
        end as alert_category,
        
        -- Distance categories
        case 
            when rg.avg_distance_miles >= 2000 then 'Long Haul'
            when rg.avg_distance_miles >= 500 then 'Medium Haul'
            else 'Short Haul'
        end as distance_category,
        
        -- Revenue per mile efficiency
        rg.total_revenue / nullif(rg.avg_distance_miles, 0) as revenue_per_mile,
        
        -- Alert rate vs. market average (for competitive analysis)
        rg.route_alert_rate - avg(rg.route_alert_rate) over () as alert_rate_vs_avg
        
    from route_geo rg
    left join route_extra_costs rec using (route)
    
    -- Only include routes with geographic data for map visualizations
    where rg.origin_lat is not null 
      and rg.origin_lon is not null
      and rg.destination_lat is not null 
      and rg.destination_lon is not null
)

select * from final_summary
order by total_bookings desc