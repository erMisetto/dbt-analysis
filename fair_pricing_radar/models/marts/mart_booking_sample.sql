{{ config(materialized='table') }}

-- Stratified sampling maintaining statistical representation across all dimensions
-- Target: ~1.5M rows from 82M (1.8% sample rate) with guaranteed minimum samples per stratum

{% set target_sample_rate = 0.018 %}
{% set min_samples_per_stratum = 5 %}
{% set max_samples_per_stratum = 100 %}

with base_data as (
    select
        legid,
        route,
        origin,
        destination,
        carrier_code,
        dtf_bucket,
        fare_bucket,
        flight_date,
        search_date,
        total_fare,
        base_fare,
        trip_dist,
        usd_per_mi,
        any_rule_flag,
        r1_price_outlier,
        r2_seat_scarcity,
        r3_monopoly_premium,
        seats_flag,
        is_nonstop,
        is_basic_econ,
        
        -- Pre-calculate expensive Tableau fields
        p50_usd_per_mi,
        p95_usd_per_mi,
        hhi,
        carrier_count
        
    from {{ ref('mart_pricing_features') }}
),

-- Calculate stratum-level statistics for sampling weights
stratum_stats as (
    select
        route,
        carrier_code,
        dtf_bucket,
        fare_bucket,
        any_rule_flag,
        
        count(*) as stratum_size,
        avg(total_fare) as stratum_avg_fare,
        stddev(total_fare) as stratum_stddev_fare,
        
        -- Determine target sample size per stratum
        greatest(
            {{ min_samples_per_stratum }}, 
            least(
                round(count(*) * {{ target_sample_rate }}),
                {{ max_samples_per_stratum }}
            )
        ) as target_samples,
        
        -- Calculate sampling probability
        greatest(
            {{ min_samples_per_stratum }}, 
            least(
                round(count(*) * {{ target_sample_rate }}),
                {{ max_samples_per_stratum }}
            )
        ) / count(*) as sampling_prob
        
    from base_data
    group by route, carrier_code, dtf_bucket, fare_bucket, any_rule_flag
),

-- Create stratified sample with proper weights
sampled_data as (
    select
        b.*,
        s.stratum_size,
        s.sampling_prob,
        
        -- Sample weight for accurate aggregation in Tableau
        1.0 / s.sampling_prob as sample_weight,
        
        -- Add random seed for reproducible sampling
        row_number() over (
            partition by b.route, b.carrier_code, b.dtf_bucket, b.fare_bucket, b.any_rule_flag 
            order by random()
        ) as rn
        
    from base_data b
    join stratum_stats s using (route, carrier_code, dtf_bucket, fare_bucket, any_rule_flag)
),

-- Pre-calculate route-level median fares for cost premium calculations
route_medians as (
    select
        route,
        carrier_code,
        dtf_bucket,
        median(total_fare) as route_median_fare
    from base_data
    group by route, carrier_code, dtf_bucket
),

-- Final sample with enhanced analytics fields
final_sample as (
    select
        s.legid,
        s.route,
        s.origin,
        s.destination,
        s.carrier_code,
        s.dtf_bucket,
        s.fare_bucket,
        s.flight_date,
        s.search_date,
        s.total_fare,
        s.base_fare,
        s.trip_dist,
        s.usd_per_mi,
        s.any_rule_flag,
        s.r1_price_outlier,
        s.r2_seat_scarcity,
        s.r3_monopoly_premium,
        s.seats_flag,
        s.is_nonstop,
        s.is_basic_econ,
        s.p50_usd_per_mi,
        s.p95_usd_per_mi,
        s.hhi,
        s.carrier_count,
        s.sample_weight,
        
        -- Pre-calculated fields for critical Tableau calculations
        rm.route_median_fare,
        
        -- Cost per booking (most critical calculation)
        case 
            when s.any_rule_flag = 1 then s.total_fare - rm.route_median_fare
            else 0 
        end as cost_premium,
        
        -- Price premium percentage
        case 
            when rm.route_median_fare > 0 then 
                (s.total_fare - rm.route_median_fare) / rm.route_median_fare * 100
            else 0 
        end as price_premium_pct,
        
        -- Route concentration category
        case 
            when s.hhi >= 0.8 then 'High Concentration'
            when s.hhi >= 0.5 then 'Moderate Concentration'
            else 'Low Concentration'
        end as concentration_level,
        
        -- Days to flight category for better performance
        case 
            when s.dtf_bucket = '00-03' then 1
            when s.dtf_bucket = '04-07' then 2  
            when s.dtf_bucket = '08-29' then 3
            when s.dtf_bucket = '30+' then 4
        end as dtf_sort_order
        
    from sampled_data s
    left join route_medians rm using (route, carrier_code, dtf_bucket)
    where s.rn <= s.target_samples  -- Apply sampling filter
)

select * from final_sample