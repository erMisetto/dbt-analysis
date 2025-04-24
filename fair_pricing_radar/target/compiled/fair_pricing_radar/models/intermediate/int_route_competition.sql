

-- ────────────────────────────────────────────────────────────────────────────────
-- 1 ·  Base rows: one record per booking
-- ────────────────────────────────────────────────────────────────────────────────
with base as (

    select
        /* canonical route key */
        origin || '-' || destination            as route,

        /* airline operating (first marketing carrier) */
        marketing_carrier,                      -- ← must exist in stg_fare_searches

        /* optional: keep flight_date so we can aggregate by period later */
        flight_date

    from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches

    /* safeguard: ignore records with no carrier code */
    where marketing_carrier is not null

),


-- ────────────────────────────────────────────────────────────────────────────────
-- 2 ·  Count bookings per carrier on each route
-- ────────────────────────────────────────────────────────────────────────────────
carrier_flights as (

    select
        route,
        marketing_carrier,
        count(*)                                as flight_bookings
    from base
    group by
        route,
        marketing_carrier

),

-- ────────────────────────────────────────────────────────────────────────────────
-- 3 ·  Competition metrics (HHI + carrier_count)
--     HHI = Σ(sᵢ²) where sᵢ = share of bookings for carrier i on the route
-- ────────────────────────────────────────────────────────────────────────────────
concentration as (

    select
        route,

        /* denominator for shares */
        sum(flight_bookings)                         as total_bookings,

        /* Herfindahl-Hirschman Index */
        sum( power(flight_bookings, 2) )
            / power(sum(flight_bookings), 2)         as hhi,

        /* # distinct carriers */
        count(marketing_carrier)                     as carrier_count
    from carrier_flights
    group by route

)

-- ────────────────────────────────────────────────────────────────────────────────
-- 4 ·  Final output
-- ────────────────────────────────────────────────────────────────────────────────
select
    route,
    carrier_count,      -- e.g. 1  = monopoly, 2+ = competing carriers
    hhi                 -- 1.0 = pure monopoly; < 0.20 = highly competitive
from concentration