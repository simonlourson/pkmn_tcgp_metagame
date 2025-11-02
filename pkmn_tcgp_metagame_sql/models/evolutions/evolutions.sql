{{ config(materialized='table') }}


with skip_intermediate_stage as (
  select
    e.previous_stage_url,
    e2.next_stage_url
  from {{ source('raw', 'evolutions') }} as e
  inner join
    {{ source('raw', 'evolutions') }} as e2
    on (e.next_stage_url = e2.previous_stage_url)
)

select
  previous_stage_url,
  next_stage_url
from {{ source('raw', 'evolutions') }}
union
select
  previous_stage_url,
  next_stage_url
from skip_intermediate_stage
