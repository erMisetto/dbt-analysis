{{ config(materialized='table') }}

select
  /* overall */
  count(*)                       as num_flagged_bookings,
  sum(extra_cost)                as total_extra_cost,
  sum(extra_cost) / nullif(count(*),0) as avg_extra_per_booking,

  /* by rule */
  sum(case when is_r1 = 1 then extra_cost else 0 end)
                                 as total_extra_r1,
  sum(case when is_r2 = 1 then extra_cost else 0 end)
                                 as total_extra_r2,
  sum(case when is_r3 = 1 then extra_cost else 0 end)
                                 as total_extra_r3

from {{ ref('int_flagged_extra_costs') }}
