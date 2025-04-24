{{ config(materialized='table') }}

{% 
  set z_cut       = var('z_threshold',      1.5)  %}
{% set r2_pct      = var('r2_pct_threshold', 0.90) %}
{% set r3_hhi_cut  = var('r3_hhi_threshold', 0.80) %}

{# derive “p<XX>_usd_per_mi” field for rule 2 #}
{% set r2_pctile  = ((r2_pct * 100) | round(0) | int) %}
{% set r2_field   = 'p' ~ r2_pctile ~ '_usd_per_mi' %}


-- 1 · BOOKINGS (clean staging)
with bookings as (
  select
    s.legid,
    s.origin,
    s.destination,
    s.origin || '-' || s.destination            as route,
    s.search_date,
    s.flight_date,
    s.days_to_flight,
    case
      when s.days_to_flight between  0 and  3 then '00-03'
      when s.days_to_flight between  4 and  7 then '04-07'
      when s.days_to_flight between  8 and 29 then '08-29'
      else                                      '30+'
    end                                          as dtf_bucket,
    s.total_fare,
    s.base_fare,
    s.total_fare / nullif(s.base_fare, 0)       as total_base_ratio,
    array_size(split(s.segmentsdepartureairportcode, '||'))
                                                as segment_count,
    split(s.segmentsairlinecode, '||')[0]        as carrier_code,
    s.seats_flag,
    s.fare_bucket,
    s.total_miles                               as staging_trip_dist,
    s.usd_per_mi                                as staging_usd_per_mi
  from {{ ref('stg_fare_searches') }} as s
),

-- 2 · LAST-RESORT DISTANCE (great-circle + 2%)
gc_dist as (
  select
    b.*,
    coalesce(
      b.staging_trip_dist,
      1.02 * (
        st_distance(
          to_geography('POINT(' || og.lon_deg || ' ' || og.lat_deg || ')'),
          to_geography('POINT(' || dg.lon_deg || ' ' || dg.lat_deg || ')')
        ) / 1609.344
      )
    )                                          as trip_dist
  from bookings b
  left join {{ ref('seed_airport_geo') }} as og
    on og.iata_code = b.origin
  left join {{ ref('seed_airport_geo') }} as dg
    on dg.iata_code = b.destination
),

-- 3 · RECALCULATE USD_PER_MI
priced as (
  select
    g.*,
    g.total_fare / nullif(g.trip_dist, 0)      as usd_per_mi
  from gc_dist g
),

-- 4 · BENCHMARKS (pull p50,p90,p95,p97 from stg_route_stats)
benchmarks as (
  select
    p.*,
    rs.p50_usd_per_mi,
    rs.p90_usd_per_mi,
    rs.p95_usd_per_mi,
    rs.p97_usd_per_mi
  from priced p
  left join {{ ref('stg_route_stats') }} as rs
    on rs.route       = p.route
   and rs.fare_bucket = p.fare_bucket
),

-- 5 · COMPETITION METRICS (clamp HHI to [0,1])
comp as (
  select
    b.*,
    c.carrier_count,
    least(1, greatest(0, c.hhi))              as hhi
  from benchmarks b
  left join {{ ref('int_route_competition') }} as c
    on c.route = b.route
),

-- 6 · RULE FLAGS (using your tuned thresholds)
flags as (
  select
    *,
    /* R1: statistical outlier (z ≥ {{ z_cut }}) */
    case
      when p50_usd_per_mi is not null
       and p95_usd_per_mi is not null
       and (usd_per_mi - p50_usd_per_mi)
           / nullif((p95_usd_per_mi - p50_usd_per_mi)/1.645,0)
           >= {{ z_cut }}
      then 1 else 0
    end as r1_price_outlier,

    /* R2: seat scarcity + top-{{ r2_pctile }}% price */
    case
      when seats_flag in ('sold_out','scarce')
       and {{ r2_field }} is not null
       and usd_per_mi >= {{ r2_field }}
      then 1 else 0
    end as r2_seat_scarcity,

    /* R3: monopoly premium (HHI ≥ {{ r3_hhi_cut }}) */
    case
      when hhi >= {{ r3_hhi_cut }}
       and p95_usd_per_mi is not null
       and usd_per_mi >= p95_usd_per_mi
      then 1 else 0
    end as r3_monopoly_premium

  from comp
),

-- 7 · FINAL SELECT (any-rule flag)
final as (
  select
    f.*,
    case
      when f.r1_price_outlier    = 1
        or f.r2_seat_scarcity    = 1
        or f.r3_monopoly_premium = 1
      then 1 else 0
    end                                             as any_rule_flag
  from flags f
)

select * from final
