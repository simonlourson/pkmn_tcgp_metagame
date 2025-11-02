{{ config(materialized='table') }}

select
  decklist_id,
  tournament_id,
  player_id,
  string_agg(
    card_url, '#'
    order by card_url
  ) as deck_id
from {{ ref("decklists_pruned") }}
group by decklist_id, tournament_id, player_id
