

with alerts as (

  select
    m.legid,
    m.route,
    m.flight_date,
    m.carrier_code,
    d.carrier_name,                     -- now pulled from dim_carrier
    m.total_fare,
    m.trip_dist,

    m.r1_price_outlier    as flag_price_outlier,
    m.r2_seat_scarcity    as flag_seat_scarcity,
    m.r3_monopoly_premium as flag_monopoly,

    array_construct_compact(
      case when m.r1_price_outlier    = 1 then 'price_outlier'    end,
      case when m.r2_seat_scarcity    = 1 then 'seat_scarcity'    end,
      case when m.r3_monopoly_premium = 1 then 'monopoly_premium' end
    ) as flag_list

  from FLIGHT_PRICES.WALRUS_PUBLIC.mart_pricing_features as m
  left join FLIGHT_PRICES.WALRUS_PUBLIC.dim_carrier as d
    on m.carrier_code = d.carrier_code

  where m.r1_price_outlier    = 1
     or m.r2_seat_scarcity    = 1
     or m.r3_monopoly_premium = 1

)

select * from alerts