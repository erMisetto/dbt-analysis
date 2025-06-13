{{ config(materialized='table') }}

-- Complete carrier coverage for performance analysis (Visualizations 3 & 4)
-- Supports: "Market Dominance" treemap and "Delta Leads" horizontal bar chart

with carrier_base as (
    select
        mpf.carrier_code,
        mpf.route,
        mpf.total_fare,
        mpf.any_rule_flag,
        mpf.r1_price_outlier,
        mpf.r2_seat_scarcity,
        mpf.r3_monopoly_premium,
        mpf.hhi,
        mpf.carrier_count,
        mpf.dtf_bucket,
        mpf.fare_bucket,
        mpf.flight_date,
        
        -- Get carrier name from dimension
        dc.carrier_name
        
    from {{ ref('mart_pricing_features') }} mpf
    left join {{ ref('dim_carrier') }} dc
        on mpf.carrier_code = dc.carrier_code
),

-- Calculate route-level median fares for extra cost calculations
route_medians as (
    select
        carrier_code,
        route,
        dtf_bucket,
        median(total_fare) as route_median_fare
    from carrier_base
    group by carrier_code, route, dtf_bucket
),

-- Core carrier metrics
carrier_metrics as (
    select
        cb.carrier_code,
        cb.carrier_name,
        
        -- Volume metrics
        count(*) as total_bookings,
        sum(cb.any_rule_flag) as flagged_bookings,
        count(distinct cb.route) as routes_served,
        count(distinct cb.flight_date) as unique_flight_dates,
        
        -- Violation rates (critical for bar chart ranking)
        round(100.0 * sum(cb.any_rule_flag) / count(*), 2) as violation_rate,
        round(100.0 * sum(cb.r1_price_outlier) / count(*), 2) as r1_violation_rate,
        round(100.0 * sum(cb.r2_seat_scarcity) / count(*), 2) as r2_violation_rate,
        round(100.0 * sum(cb.r3_monopoly_premium) / count(*), 2) as r3_violation_rate,
        
        -- Raw violation counts for treemap sizing
        sum(cb.r1_price_outlier) as r1_violations,
        sum(cb.r2_seat_scarcity) as r2_violations,
        sum(cb.r3_monopoly_premium) as r3_violations,
        
        -- Financial metrics
        sum(cb.total_fare) as total_revenue,
        avg(cb.total_fare) as avg_fare,
        median(cb.total_fare) as median_fare,
        avg(case when cb.any_rule_flag = 1 then cb.total_fare else null end) as avg_flagged_fare,
        
        -- Market share calculations
        100.0 * count(*) / sum(count(*)) over () as market_share_bookings,
        100.0 * sum(cb.total_fare) / sum(sum(cb.total_fare)) over () as market_share_revenue,
        
        -- Competition metrics
        avg(cb.hhi) as avg_route_concentration,
        count(case when cb.hhi >= 0.8 then 1 end) as monopoly_route_bookings,
        100.0 * count(case when cb.hhi >= 0.8 then 1 end) / count(*) as pct_monopoly_bookings
        
    from carrier_base cb
    group by cb.carrier_code, cb.carrier_name
),

-- Calculate extra costs with proper route-level medians
carrier_extra_costs as (
    select
        cb.carrier_code,
        sum(case 
            when cb.any_rule_flag = 1 and rm.route_median_fare is not null then 
                cb.total_fare - rm.route_median_fare
            else 0 
        end) as total_extra_cost,
        
        avg(case 
            when cb.any_rule_flag = 1 and rm.route_median_fare is not null then 
                cb.total_fare - rm.route_median_fare
            else null 
        end) as avg_extra_cost_per_violation
        
    from carrier_base cb
    left join route_medians rm 
        on cb.carrier_code = rm.carrier_code 
        and cb.route = rm.route
        and cb.dtf_bucket = rm.dtf_bucket
    group by cb.carrier_code
),

-- Performance by fare bucket for detailed analysis
carrier_fare_bucket_breakdown as (
    select
        carrier_code,
        
        -- Premium cabin performance
        sum(case when fare_bucket = 'prem_cabin' then any_rule_flag else 0 end) as prem_violations,
        count(case when fare_bucket = 'prem_cabin' then 1 end) as prem_bookings,
        
        -- Full/mid economy performance  
        sum(case when fare_bucket = 'full_mid_econ' then any_rule_flag else 0 end) as full_violations,
        count(case when fare_bucket = 'full_mid_econ' then 1 end) as full_bookings,
        
        -- Deep discount economy performance
        sum(case when fare_bucket = 'deep_disc_econ' then any_rule_flag else 0 end) as disc_violations,
        count(case when fare_bucket = 'deep_disc_econ' then 1 end) as disc_bookings
        
    from carrier_base
    group by carrier_code
),

-- Final enhanced carrier summary
final_summary as (
    select
        cm.*,
        coalesce(cec.total_extra_cost, 0) as total_extra_cost,
        coalesce(cec.avg_extra_cost_per_violation, 0) as avg_extra_cost_per_violation,
        
        -- Fare bucket violation rates
        round(100.0 * cfb.prem_violations / nullif(cfb.prem_bookings, 0), 2) as prem_violation_rate,
        round(100.0 * cfb.full_violations / nullif(cfb.full_bookings, 0), 2) as full_violation_rate,
        round(100.0 * cfb.disc_violations / nullif(cfb.disc_bookings, 0), 2) as disc_violation_rate,
        
        -- Performance rankings (for Tableau rank calculations)
        rank() over (order by cm.violation_rate desc) as violation_rate_rank,
        rank() over (order by cm.total_revenue desc) as revenue_rank,
        rank() over (order by cm.total_bookings desc) as volume_rank,
        rank() over (order by coalesce(cec.total_extra_cost, 0) desc) as extra_cost_rank,
        
        -- Categorical classifications
        case 
            when cm.market_share_bookings >= 15 then 'Major Carrier'
            when cm.market_share_bookings >= 5 then 'Mid-tier Carrier'
            else 'Small Carrier'
        end as carrier_size_category,
        
        case 
            when cm.violation_rate >= 25 then 'High Violation'
            when cm.violation_rate >= 15 then 'Medium Violation'
            else 'Low Violation'
        end as violation_category,
        
        case 
            when cm.avg_route_concentration >= 0.7 then 'High Concentration'
            when cm.avg_route_concentration >= 0.4 then 'Medium Concentration'
            else 'Low Concentration'
        end as concentration_category,
        
        -- Financial impact per booking
        coalesce(cec.total_extra_cost, 0) / nullif(cm.total_bookings, 0) as extra_cost_per_booking,
        
        -- Market dominance indicators
        case when cm.market_share_bookings >= 20 then 1 else 0 end as is_market_leader,
        case when cm.violation_rate >= 30 then 1 else 0 end as is_high_violator,
        
        -- Efficiency metrics
        cm.total_revenue / nullif(cm.total_bookings, 0) as revenue_per_booking,
        cm.routes_served / nullif(cm.total_bookings, 0) * 1000 as route_diversity_index
        
    from carrier_metrics cm
    left join carrier_extra_costs cec using (carrier_code)
    left join carrier_fare_bucket_breakdown cfb using (carrier_code)
)

select * from final_summary
order by violation_rate desc, total_bookings desc