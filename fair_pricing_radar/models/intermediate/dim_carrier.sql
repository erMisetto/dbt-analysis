{{ config(materialized='table') }}

select
  code   as carrier_code,
  name   as carrier_name
from {{ ref('carrier_mapping') }}
