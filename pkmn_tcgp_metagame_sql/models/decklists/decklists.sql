{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['tournament_id', 'player_id']) }} as decklist_id,
  tournament_id,
  player_id,
  card_url,
  decklist_count
from {{ source('raw', 'decklists') }}
