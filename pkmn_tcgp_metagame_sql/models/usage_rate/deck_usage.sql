{{ config(materialized='table') }}

select
  t.set_code,
  da.deck_id,
  count(*)::int as nb_decklists,
  rank() over (
    partition by t.set_code
    order by count(*) desc
  )::int
  as usage_rank,
  count(*)::float / ndps.nb_decklists as usage_rate
from {{ ref("decklists_aggregated") }} as da
inner join {{ ref("tournaments") }} as t on da.tournament_id = t.tournament_id
inner join
  {{ ref("nb_decklists_per_season") }} as ndps
  on t.set_code = ndps.set_code
group by t.set_code, da.deck_id, ndps.nb_decklists
