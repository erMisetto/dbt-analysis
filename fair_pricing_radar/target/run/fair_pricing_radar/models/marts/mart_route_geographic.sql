
  
    

        create or replace transient table FLIGHT_PRICES.WALRUS_PUBLIC.mart_route_geographic
         as
        (

WITH
    ---------------------------------------------------------------------------
    -- 1. Base route-level aggregates
    ---------------------------------------------------------------------------
    route_metrics AS (
        SELECT
            f.route,
            SPLIT_PART(f.route, '-', 1) AS origin,
            SPLIT_PART(f.route, '-', 2) AS destination,
            COUNT(*)                        AS total_bookings,
            SUM(any_rule_flag)              AS flagged_bookings,
            100.0 * SUM(any_rule_flag) / COUNT(*) AS pct_flagged,
            AVG(total_fare)                 AS avg_fare
        FROM FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features f
        GROUP BY f.route
    ),

    ---------------------------------------------------------------------------
    -- 2. Per-booking median(total_fare)
    ---------------------------------------------------------------------------
    route_medians AS (
        SELECT
            route,
            MEDIAN(total_fare) OVER (PARTITION BY route) AS median_fare
        FROM FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features
    ),

    ---------------------------------------------------------------------------
    -- 3. Collapse to one row per route
    ---------------------------------------------------------------------------
    one_median_per_route AS (
        SELECT
            route,
            MAX(median_fare) AS median_fare
        FROM route_medians
        GROUP BY route
    ),

    ---------------------------------------------------------------------------
    -- 4. Join median back, compute route-level extra cost
    ---------------------------------------------------------------------------
    route_with_median AS (
        SELECT
            rm.route,
            rm.origin,
            rm.destination,
            rm.total_bookings,
            rm.flagged_bookings,
            rm.pct_flagged,
            rm.avg_fare,
            om.median_fare,
            SUM(
                CASE
                    WHEN f.any_rule_flag = 1
                         THEN f.total_fare - om.median_fare
                    ELSE 0
                END
            ) AS total_extra_cost
        FROM FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features f
        JOIN route_metrics        rm ON f.route = rm.route
        JOIN one_median_per_route om ON rm.route = om.route
        GROUP BY
            rm.route,
            rm.origin,
            rm.destination,
            rm.total_bookings,
            rm.flagged_bookings,
            rm.pct_flagged,
            rm.avg_fare,
            om.median_fare
    ),

    ---------------------------------------------------------------------------
    -- 5. Merge legacy 24-row seed with full airport_coordinates CSV
    ---------------------------------------------------------------------------
    airport_seed_union AS (
        SELECT
            UPPER(iata_code)       AS iata_code,
            lat_deg                AS airport_lat,
            lon_deg                AS airport_lon
        FROM FLIGHT_PRICES.WALRUS_PUBLIC.seed_airport_geo

        UNION ALL

        SELECT
            UPPER(iata_code)       AS iata_code,
            latitude               AS airport_lat,
            longitude              AS airport_lon
        FROM FLIGHT_PRICES.WALRUS_PUBLIC.airport_coordinates
        WHERE   iata_code IS NOT NULL
            AND latitude  IS NOT NULL
            AND longitude IS NOT NULL
    ),

    airport_geo AS (
        SELECT
            iata_code,
            MAX(airport_lat) AS airport_lat,
            MAX(airport_lon) AS airport_lon
        FROM airport_seed_union
        GROUP BY iata_code
    )

SELECT
    r.route,
    r.origin,
    r.destination,
    r.total_bookings,
    r.flagged_bookings,
    r.pct_flagged,
    r.avg_fare,
    r.total_extra_cost,
    og.airport_lat  AS origin_lat,
    og.airport_lon  AS origin_lon,
    dg.airport_lat  AS destination_lat,
    dg.airport_lon  AS destination_lon
FROM route_with_median r
LEFT JOIN airport_geo og ON r.origin      = og.iata_code
LEFT JOIN airport_geo dg ON r.destination = dg.iata_code
WHERE
      og.airport_lat  IS NOT NULL
  AND og.airport_lon  IS NOT NULL
  AND dg.airport_lat  IS NOT NULL
  AND dg.airport_lon  IS NOT NULL
  AND r.total_bookings >= 1000
        );
      
  