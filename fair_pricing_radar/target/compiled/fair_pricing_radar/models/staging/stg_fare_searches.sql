




-- ╔══════════════════════════════════════════════╗
-- 1 · RAW rows (keep only sensible bookings)
-- ╚══════════════════════════════════════════════╝
with raw as (

    select
        legid,

        try_to_date(searchdate)                  as search_date,
        try_to_date(flightdate)                  as flight_date,

        cast(totalfare         as numeric)       as total_fare,
        cast(basefare          as numeric)       as base_fare,
        cast(seatsremaining    as numeric)       as seats_remaining,

        isnonstop                                as is_nonstop,
        isbasiceconomy                           as is_basic_econ,
        isrefundable                             as is_refundable,
        trim(farebasiscode)                      as farebasiscode,

        cast(totaltraveldistance as numeric)     as raw_total_miles,

        travelduration,
        segmentsdepartureairportcode,
        segmentsarrivalairportcode,
        segmentsairlinecode,
        segmentsdistance,
        segmentsdurationinseconds,
        segmentsequipmentdescription,
        segmentscabincode

    from FLIGHT_PRICES.WALRUS_PUBLIC.FLIGHT_PRICING_DATA
    where   searchdate    is not null
        and flightdate    is not null
        and flightdate   >= searchdate
        and totalfare     is not null

),

-- ╔══════════════════════════════════════════════╗
-- 2 · BASIC DERIVATIONS
-- ╚══════════════════════════════════════════════╝
prep as (

    select
        r.*,

        split(trim(r.segmentsdepartureairportcode), '||') as dep_arr,
        split(trim(r.segmentsarrivalairportcode),   '||') as arr_arr,

        /* origin / destination (first & last element) */
        trim(replace(dep_arr[0],                       '"',''))              as origin,
        trim(replace(arr_arr[array_size(arr_arr)-1],   '"',''))              as destination,

        /* CLEAN marketing carrier (1st code, no stray quotes) */
        replace(split(trim(r.segmentsairlinecode), '||')[0], '"','')         as marketing_carrier,

        /* use the aliased columns, not raw SEARCHDATE */
        datediff('day', r.search_date, r.flight_date)                         as days_to_flight

    from raw r

),

-- ╔══════════════════════════════════════════════╗
-- 3 · ADD AIRPORT GEO (for GC fallback)
-- ╚══════════════════════════════════════════════╝
add_geo as (

    select
        p.*,
        og.lat_deg   as orig_lat,
        og.lon_deg   as orig_lon,
        dg.lat_deg   as dest_lat,
        dg.lon_deg   as dest_lon

    from prep p
    left join FLIGHT_PRICES.WALRUS_PUBLIC.seed_airport_geo og on p.origin      = og.iata_code
    left join FLIGHT_PRICES.WALRUS_PUBLIC.seed_airport_geo dg on p.destination = dg.iata_code
),

-- ╔══════════════════════════════════════════════╗
-- 4 · DISTANCE: choose “reasonable” miles
--      • raw_total_miles  (path) kept **iff**
--        it is ≤ 2 × GC; otherwise fall back to GC
--      • GC always padded by 2 % to approximate routing
-- ╚══════════════════════════════════════════════╝
with_gc as (

    select
        g.*,

        /* great-circle miles (+2 % pad) */
        case
          when g.orig_lat is not null and g.dest_lat is not null then
               1.02 * st_distance(
                       to_geography('POINT(' || g.orig_lon || ' ' || g.orig_lat || ')'),
                       to_geography('POINT(' || g.dest_lon || ' ' || g.dest_lat || ')')
                     ) / 1609.344
        end                                                    as gc_miles

    from add_geo g
),

choose_dist as (

    select
        c.*,

        coalesce(
            -- 1️⃣ keep path distance if “reasonable”
            case
              when c.raw_total_miles is not null
               and c.gc_miles        is not null
               and c.raw_total_miles <= 2 * c.gc_miles
              then c.raw_total_miles
            end,
            -- 2️⃣ else GC-miles
            c.gc_miles,
            -- 3️⃣ else raw (last resort)
            c.raw_total_miles
        )                                                       as total_miles

    from with_gc c
),

-- ╔══════════════════════════════════════════════╗
-- 5 · BUSINESS FIELDS
-- ╚══════════════════════════════════════════════╝
ready as (

    select
        d.*,

        d.total_fare / nullif(d.total_miles,0)                  as usd_per_mi,

        case
            when seats_remaining = 0          then 'sold_out'
            when seats_remaining between 1 and 8 then 'scarce'
            else                                   '9plus'
        end                                                    as seats_flag,

        fb.fare_bucket
    from choose_dist d
    left join FLIGHT_PRICES.WALRUS_PUBLIC.seed_fare_bucket_map fb
      on upper(left(d.farebasiscode,1)) = fb.first_letter
)

-- ╔══════════════════════════════════════════════╗
-- 6 · FINAL STAGING SELECT
-- ╚══════════════════════════════════════════════╝
select
    legid,
    search_date,
    flight_date,
    days_to_flight,

    origin,
    destination,
    origin || '-' || destination                          as route,

    marketing_carrier,

    total_fare,
    base_fare,
    total_miles,
    usd_per_mi,

    is_nonstop,
    is_basic_econ,
    is_refundable,

    farebasiscode,
    fare_bucket,

    seats_remaining,
    seats_flag,

    travelduration,
    segmentsdepartureairportcode,
    segmentsarrivalairportcode,
    segmentsairlinecode,
    segmentsdistance,
    segmentsdurationinseconds,
    segmentsequipmentdescription,
    segmentscabincode

from ready