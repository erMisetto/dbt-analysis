





-- 1. Core booking row + route & carrier stats
with base as (

  select
    s.legid,
    s.route,
    s.flight_date,
    s.search_date,
    s.days_to_flight,
    s.fare_bucket,
    s.usd_per_mi,
    s.seats_flag,

    rs.p50_usd_per_mi,
    rs.p90_usd_per_mi,
    rs.p95_usd_per_mi,
    rs.p97_usd_per_mi,

    c.carrier_count,
    c.hhi

  from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches           as s
  left join FLIGHT_PRICES.WALRUS_PUBLIC.stg_route_stats       as rs using (route, fare_bucket)
  left join FLIGHT_PRICES.WALRUS_PUBLIC.int_route_competition as c  using (route)

),

-- 2. Apply Rules 1–3 with dynamic Rule 2 threshold
rules as (

  select
    *,
    
    /* Rule 1: z-score ≥ 1.5 */
    case
      when ((usd_per_mi - p50_usd_per_mi)
             / nullif((p95_usd_per_mi - p50_usd_per_mi) / 1.645, 0)
           ) >= 1.5
      then 1 else 0
    end as r1_price_outlier_flag,

    /* Rule 2: seat scarcity + price ≥ dynamic 90.0th percentile */
    case
      when seats_flag in ('sold_out','scarce')
       and usd_per_mi >= percentile_cont(0.9)
                            within group (order by usd_per_mi)
                            over (partition by route, fare_bucket)
      then 1 else 0
    end as r2_seat_scarcity_flag,

    /* Rule 3: monopoly premium (HHI ≥ 0.8) */
    case
      when hhi >= 0.8
       and usd_per_mi >= p95_usd_per_mi
      then 1 else 0
    end as r3_monopoly_premium_flag

  from base

)

-- 3. Final select with “any rule” flag
select
  *,
  case
    when r1_price_outlier_flag    = 1
      or r2_seat_scarcity_flag    = 1
      or r3_monopoly_premium_flag = 1
    then 1 else 0
  end as any_rule_flag

from rules