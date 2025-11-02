{{ config(materialized='table') }}

with next_evolution as (
  select
    d_previous.decklist_id,
    d_previous.card_url,
    d_next.card_url as next_stage_card_url
  from {{ ref("decklists") }} as d_previous
  inner join
    {{ ref("evolutions") }} as e
    on (d_previous.card_url = e.previous_stage_url)
  inner join
    {{ ref("decklists") }} as d_next
    on
      (
        d_previous.decklist_id = d_next.decklist_id
        and e.next_stage_url = d_next.card_url
      )
),

prep_for_exclude as (
  select
    d.decklist_id,
    d.tournament_id,
    d.player_id,
    d.card_url,
    d.decklist_count,
    c.card_type,
    ne.next_stage_card_url
  from {{ ref("decklists") }} as d
  inner join {{ ref("cards") }} as c on (d.card_url = c.card_url)
  left outer join
    next_evolution as ne
    on (d.decklist_id = ne.decklist_id and d.card_url = ne.card_url)
)

select
  decklist_id,
  tournament_id,
  player_id,
  card_url,
  decklist_count
from prep_for_exclude
where
  card_type = 'Pokémon'
  and next_stage_card_url is null
