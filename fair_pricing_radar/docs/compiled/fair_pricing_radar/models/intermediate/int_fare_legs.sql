

with bookings as (

    /* Pull the raw pipe-delimited strings from staging */
    select
        legid,

        -- turn each pipe-delimited string into an ARRAY
        split(trim(segmentsdepartureairportcode), '||')  as dep_airport_arr,
        split(trim(segmentsarrivalairportcode),   '||')  as arr_airport_arr,
        split(trim(segmentsairlinecode),          '||')  as seg_airline_arr,
        split(trim(segmentsdistance),             '||')  as seg_dist_arr,
        split(trim(segmentsdurationinseconds),    '||')  as seg_dur_arr

    from FLIGHT_PRICES.WALRUS_PUBLIC.stg_fare_searches

), flattened as (

    /*
     * FLATTEN explodes the arrays.  `f.index` is the 0-based position.
     * For each position, pull the matching element from every array.
     */
    select
        b.legid,
        f.index + 1                                                   as leg_number,

        trim(replace(b.dep_airport_arr[f.index], '\"',''))            as departure_airport,
        trim(replace(b.arr_airport_arr[f.index], '\"',''))            as arrival_airport,
        trim(replace(b.seg_airline_arr[f.index],  '\"',''))           as operating_carrier,

        try_to_number(
            trim(replace(b.seg_dist_arr[f.index], '\"',''))
        )                                                             as leg_distance,

        try_to_number(
            trim(replace(b.seg_dur_arr[f.index],  '\"',''))
        )                                                             as leg_duration_sec

    from bookings b,
         lateral flatten(input => b.dep_airport_arr) f                -- drive the explode

)

select
    legid,
    leg_number,
    departure_airport,
    arrival_airport,
    operating_carrier,
    leg_distance,
    leg_duration_sec
from flattened