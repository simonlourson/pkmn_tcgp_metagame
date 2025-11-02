{{ config(materialized='table') }}

with max_set_code as (
  select
    d.tournament_id,
    max(c.set_code) as set_code
  from {{ ref("decklists") }} as d
  inner join {{ ref("cards") }} as c on d.card_url = c.card_url
  where c.set_code <> 'P-A'
  group by d.tournament_id
),

with_day_in_season as (
  select
    t.tournament_id,
    max_set_code.set_code,
    t.tournament_name,
    t.tournament_organizer,
    t.tournament_date,
    dense_rank()
      over (
        partition by max_set_code.set_code
        order by t.tournament_date::date
      )
    ::int as day_in_season
  from {{ source('raw', 'tournaments') }} as t
  inner join max_set_code on (t.tournament_id = max_set_code.tournament_id)
)

select
  tournament_id,
  set_code,
  tournament_name,
  tournament_organizer,
  tournament_date,
  day_in_season,
  (day_in_season - (day_in_season % 8)) / 8 as week_in_season
from with_day_in_season
