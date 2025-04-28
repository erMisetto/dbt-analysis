{{ config(materialized='table') }}

WITH route_metrics AS (
    SELECT 
        f.ROUTE,
        SPLIT_PART(f.ROUTE, '-', 1) AS ORIGIN,
        SPLIT_PART(f.ROUTE, '-', 2) AS DESTINATION,
        COUNT(*) AS TOTAL_BOOKINGS,
        SUM(CASE WHEN f.R1_PRICE_OUTLIER = 1 OR f.R2_SEAT_SCARCITY = 1 OR f.R3_MONOPOLY_PREMIUM = 1 
            THEN 1 ELSE 0 END) AS FLAGGED_BOOKINGS,
        ROUND(100 * SUM(CASE WHEN f.R1_PRICE_OUTLIER = 1 OR f.R2_SEAT_SCARCITY = 1 OR f.R3_MONOPOLY_PREMIUM = 1 
            THEN 1 ELSE 0 END) / COUNT(*), 1) AS PCT_FLAGGED
    FROM {{ ref('mart_pricing_features') }} f
    GROUP BY f.ROUTE
),
route_comp AS (
    SELECT 
        r.ROUTE,
        r.HHI,
        CASE 
            WHEN r.HHI >= 0.8 THEN 'High Concentration'
            WHEN r.HHI >= 0.5 THEN 'Moderate Concentration'
            ELSE 'Low Concentration'
        END AS CONCENTRATION_LEVEL
    FROM {{ ref('int_route_competition') }} r
),
airport_geo AS (
    SELECT 
        IATA_CODE,
        LAT_DEG AS AIRPORT_LAT,
        LON_DEG AS AIRPORT_LON
    FROM {{ ref('seed_airport_geo') }}
)
SELECT 
    m.ROUTE,
    m.ORIGIN,
    m.DESTINATION,
    m.TOTAL_BOOKINGS,
    m.FLAGGED_BOOKINGS,
    m.PCT_FLAGGED,
    c.HHI,
    c.CONCENTRATION_LEVEL,
    o.AIRPORT_LAT AS ORIGIN_LAT,
    o.AIRPORT_LON AS ORIGIN_LON,
    d.AIRPORT_LAT AS DESTINATION_LAT,
    d.AIRPORT_LON AS DESTINATION_LON
FROM route_metrics m
LEFT JOIN route_comp c ON m.ROUTE = c.ROUTE
LEFT JOIN airport_geo o ON m.ORIGIN = o.IATA_CODE
LEFT JOIN airport_geo d ON m.DESTINATION = d.IATA_CODE
WHERE o.AIRPORT_LAT IS NOT NULL 
  AND o.AIRPORT_LON IS NOT NULL
  AND d.AIRPORT_LAT IS NOT NULL 
  AND d.AIRPORT_LON IS NOT NULL