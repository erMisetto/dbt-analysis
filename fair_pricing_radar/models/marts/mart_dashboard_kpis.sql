{{ config(materialized='table') }}

-- 1) Base data: every booking, annotated with alert flags & extra cost
with base as (

  select
    s.legid,
    s.search_date,
    s.flight_date,
    s.route,
    s.fare_bucket,
    s.usd_per_mi,
    coalesce(f.any_rule_flag,         0) as any_alert,
    coalesce(f.r1_price_outlier,      0) as is_r1_outlier,
    coalesce(f.r2_seat_scarcity,      0) as is_r2_scarcity,
    coalesce(f.r3_monopoly_premium,   0) as is_r3_monopoly,
    coalesce(e.extra_cost,            0) as extra_cost

  from {{ ref('stg_fare_searches') }}         as s
  left join {{ ref('mart_pricing_features') }} as f using (legid)
  left join {{ ref('int_flagged_extra_costs') }} as e using (legid)

),

-- 2) Aggregate KPIs over that base set
kpis as (

  select
    count(*)                                   as total_bookings,
    sum(any_alert)                             as total_any_alerts,
    sum(is_r1_outlier)                         as total_R1_alerts,
    sum(is_r2_scarcity)                        as total_R2_alerts,
    sum(is_r3_monopoly)                        as total_R3_alerts,

    round(100.0 * sum(any_alert)/nullif(count(*),0),2)   as pct_any_alerts,
    round(100.0 * sum(is_r1_outlier)/nullif(count(*),0),2) as pct_R1_outliers,
    round(100.0 * sum(is_r2_scarcity)/nullif(count(*),0),2) as pct_R2_scarcity,
    round(100.0 * sum(is_r3_monopoly)/nullif(count(*),0),2) as pct_R3_monopoly,

    sum(extra_cost)                            as total_extra_cost_usd,
    round(sum(extra_cost)/nullif(sum(any_alert),0),2) as avg_extra_per_alert

  from base

)

select * from kpis
