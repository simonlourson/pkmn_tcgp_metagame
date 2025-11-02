{{ config(materialized='table') }}

select
  t.set_code,
  da.deck_id,
  d.card_url,
  count(*)::int as nb_decklists,
  sum(d.decklist_count)::float / ndpspd.nb_decklists as average_count,
  count(*)::float / ndpspd.nb_decklists as usage_rate
from {{ ref("decklists") }} as d
inner join {{ ref("tournaments") }} as t on d.tournament_id = t.tournament_id
inner join
  {{ ref("decklists_aggregated") }} as da
  on d.decklist_id = da.decklist_id
inner join
  {{ ref("nb_decklists_per_season_per_deck") }} as ndpspd
  on t.set_code = ndpspd.set_code and da.deck_id = ndpspd.deck_id
group by t.set_code, da.deck_id, d.card_url, ndpspd.nb_decklists
