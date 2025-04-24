{{ config(materialized = 'table') }}

-- ╭──────────────────────────────────────────────────────────╮
-- 1.  Booking-level facts
-- ╰──────────────────────────────────────────────────────────╯
with base as (

    select
        legid,
        search_date,
        flight_date,
        days_to_flight,
        route,
        origin,
        destination,
        fare_bucket,
        total_miles,
        usd_per_mi,
        total_fare,
        base_fare,
        seats_flag,
        is_nonstop,
        is_basic_econ,
        is_refundable
    from {{ ref('stg_fare_searches') }}
    where usd_per_mi is not null

),

-- ╭──────────────────────────────────────────────────────────╮
-- 2.  Route-bucket benchmarks
-- ╰──────────────────────────────────────────────────────────╯
bench as (

    select *
    from {{ ref('stg_route_stats') }}

),

-- ╭──────────────────────────────────────────────────────────╮
-- 3.  Join + z-scores & premium flags
-- ╰──────────────────────────────────────────────────────────╯
joined as (

    select
        b.*,

        -- bring in benchmarks
        br.p50_usd_per_mi,
        br.p90_usd_per_mi,
        br.p95_usd_per_mi,
        br.p97_usd_per_mi,
        br.max_usd_per_mi,
        coalesce(br.legs_sampled, 0)          as legs_sampled,

        -- σ ≈ (p95 – p50) / 1.645
        (b.usd_per_mi - br.p50_usd_per_mi)
          / nullif((br.p95_usd_per_mi - br.p50_usd_per_mi) / 1.645, 0)
                                              as usd_per_mi_z,

        case when b.usd_per_mi >= br.p95_usd_per_mi then 1 else 0 end
                                              as is_above_p95,
        case when b.usd_per_mi >= br.p97_usd_per_mi then 1 else 0 end
                                              as is_above_p97

    from base  b
    left join bench br
      on  b.route       = br.route
      and b.fare_bucket = br.fare_bucket

)

select * from joined