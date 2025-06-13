{{ config(materialized='table') }}

-- Complete DTF×Fare bucket coverage for heat-map analysis (Visualization 2)
-- Supports: "45% Violations on Last-Minute Premium Bookings" heat-map

with dtf_fare_base as (
    select
        dtf_bucket,
        fare_bucket,
        total_fare,
        any_rule_flag,
        r1_price_outlier,
        r2_seat_scarcity,
        r3_monopoly_premium,
        carrier_code,
        route,
        flight_date
    from {{ ref('mart_pricing_features') }}
    where dtf_bucket is not null 
      and fare_bucket is not null
),

-- Core crosstab metrics
crosstab_metrics as (
    select
        dtf_bucket,
        fare_bucket,
        
        -- Volume metrics
        count(*) as total_bookings,
        sum(any_rule_flag) as flagged_bookings,
        count(distinct carrier_code) as unique_carriers,
        count(distinct route) as unique_routes,
        count(distinct flight_date) as unique_flight_dates,
        
        -- Alert rates (critical for heat-map coloring)
        round(100.0 * sum(any_rule_flag) / count(*), 2) as dtf_bucket_alert_rate,
        round(100.0 * sum(r1_price_outlier) / count(*), 2) as r1_rate,
        round(100.0 * sum(r2_seat_scarcity) / count(*), 2) as r2_rate,
        round(100.0 * sum(r3_monopoly_premium) / count(*), 2) as r3_rate,
        
        -- Financial metrics
        avg(total_fare) as avg_fare,
        median(total_fare) as median_fare,
        stddev(total_fare) as stddev_fare,
        min(total_fare) as min_fare,
        max(total_fare) as max_fare,
        
        -- Flagged vs non-flagged fare comparison
        avg(case when any_rule_flag = 1 then total_fare else null end) as avg_flagged_fare,
        avg(case when any_rule_flag = 0 then total_fare else null end) as avg_normal_fare,
        
        -- Premium calculations
        avg(case when any_rule_flag = 1 then total_fare else null end) - 
        avg(case when any_rule_flag = 0 then total_fare else null end) as avg_premium_amount
        
    from dtf_fare_base
    group by dtf_bucket, fare_bucket
),

-- Ensure complete coverage of all combinations
all_combinations as (
    select
        dtf.dtf_bucket,
        fb.fare_bucket
    from (
        select distinct dtf_bucket from dtf_fare_base
        union all
        values ('00-03'), ('04-07'), ('08-29'), ('30+')
    ) dtf
    cross join (
        select distinct fare_bucket from dtf_fare_base
        union all  
        values ('prem_cabin'), ('full_mid_econ'), ('deep_disc_econ')
    ) fb
),

-- Market share within each cell
market_share_analysis as (
    select
        dtf_bucket,
        fare_bucket,
        
        -- Market concentration within each DTF×Fare bucket
        count(distinct carrier_code) as competing_carriers,
        
        -- Top carrier market share in each cell
        max(carrier_bookings) as top_carrier_bookings,
        sum(carrier_bookings) as total_cell_bookings,
        round(100.0 * max(carrier_bookings) / sum(carrier_bookings), 2) as top_carrier_share
        
    from (
        select
            dtf_bucket,
            fare_bucket,
            carrier_code,
            count(*) as carrier_bookings
        from dtf_fare_base
        group by dtf_bucket, fare_bucket, carrier_code
    ) carrier_breakdown
    group by dtf_bucket, fare_bucket
),

-- Final enhanced crosstab summary
final_summary as (
    select
        ac.dtf_bucket,
        ac.fare_bucket,
        
        -- Core metrics (with zeros for missing combinations)
        coalesce(cm.total_bookings, 0) as total_bookings,
        coalesce(cm.flagged_bookings, 0) as flagged_bookings,
        coalesce(cm.dtf_bucket_alert_rate, 0) as dtf_bucket_alert_rate,
        coalesce(cm.r1_rate, 0) as r1_rate,
        coalesce(cm.r2_rate, 0) as r2_rate,
        coalesce(cm.r3_rate, 0) as r3_rate,
        
        -- Financial metrics
        cm.avg_fare,
        cm.median_fare,
        cm.stddev_fare,
        cm.min_fare,
        cm.max_fare,
        cm.avg_flagged_fare,
        cm.avg_normal_fare,
        coalesce(cm.avg_premium_amount, 0) as avg_premium_amount,
        
        -- Market structure
        coalesce(cm.unique_carriers, 0) as unique_carriers,
        coalesce(cm.unique_routes, 0) as unique_routes,
        coalesce(cm.unique_flight_dates, 0) as unique_flight_dates,
        coalesce(msa.competing_carriers, 0) as competing_carriers,
        coalesce(msa.top_carrier_share, 0) as top_carrier_share,
        
        -- Categorical fields for Tableau filters and grouping
        case 
            when ac.dtf_bucket = '00-03' then 'Last Minute'
            when ac.dtf_bucket = '04-07' then 'Short Notice'
            when ac.dtf_bucket = '08-29' then 'Standard Booking'
            when ac.dtf_bucket = '30+' then 'Advance Booking'
        end as booking_window_category,
        
        case 
            when ac.fare_bucket = 'prem_cabin' then 'Premium Cabin'
            when ac.fare_bucket = 'full_mid_econ' then 'Full/Mid Economy'
            when ac.fare_bucket = 'deep_disc_econ' then 'Deep Discount Economy'
        end as fare_category_display,
        
        -- Sort orders for proper Tableau display
        case 
            when ac.dtf_bucket = '00-03' then 1
            when ac.dtf_bucket = '04-07' then 2
            when ac.dtf_bucket = '08-29' then 3
            when ac.dtf_bucket = '30+' then 4
        end as dtf_sort_order,
        
        case 
            when ac.fare_bucket = 'prem_cabin' then 1
            when ac.fare_bucket = 'full_mid_econ' then 2
            when ac.fare_bucket = 'deep_disc_econ' then 3
        end as fare_sort_order,
        
        -- Alert rate categories for color coding
        case 
            when coalesce(cm.dtf_bucket_alert_rate, 0) >= 40 then 'High Alert'
            when coalesce(cm.dtf_bucket_alert_rate, 0) >= 20 then 'Medium Alert'
            when coalesce(cm.dtf_bucket_alert_rate, 0) >= 5 then 'Low Alert'
            else 'Minimal Alert'
        end as alert_rate_category,
        
        -- Volume categories for heat-map sizing
        case 
            when coalesce(cm.total_bookings, 0) >= 1000000 then 'Very High Volume'
            when coalesce(cm.total_bookings, 0) >= 100000 then 'High Volume'
            when coalesce(cm.total_bookings, 0) >= 10000 then 'Medium Volume'
            when coalesce(cm.total_bookings, 0) >= 1000 then 'Low Volume'
            else 'Minimal Volume'
        end as volume_category,
        
        -- Premium percentage for tooltip calculations
        case 
            when cm.avg_normal_fare > 0 then 
                round(100.0 * coalesce(cm.avg_premium_amount, 0) / cm.avg_normal_fare, 2)
            else 0 
        end as premium_percentage,
        
        -- Market share indicators
        case when coalesce(msa.top_carrier_share, 0) >= 50 then 1 else 0 end as is_dominated_cell,
        case when coalesce(msa.competing_carriers, 0) <= 2 then 1 else 0 end as is_low_competition
        
    from all_combinations ac
    left join crosstab_metrics cm using (dtf_bucket, fare_bucket)
    left join market_share_analysis msa using (dtf_bucket, fare_bucket)
)

select * from final_summary
order by dtf_sort_order, fare_sort_order