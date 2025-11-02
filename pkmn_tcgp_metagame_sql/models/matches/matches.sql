{{ 
  config(
    materialized='table',
    indexes=[
      {'columns': ['winner_decklist_id'], 'type': 'hash'},
      {'columns': ['loser_decklist_id'], 'type': 'hash'},
    ]
  )
}}

with all_matches as (
  select
    tournament_id,
    {{ dbt_utils.generate_surrogate_key(['tournament_id', 'winner_player_id']) }} as winner_decklist_id,
    winner_player_id,
    {{ dbt_utils.generate_surrogate_key(['tournament_id', 'loser_player_id']) }} as loser_decklist_id,
    loser_player_id
  from {{ source('raw', 'matches') }}
)

select all_matches.*
from all_matches
inner join {{ ref("decklists_aggregated") }} as dw on (all_matches.winner_decklist_id = dw.decklist_id)
inner join {{ ref("decklists_aggregated") }} as dl on (all_matches.loser_decklist_id = dl.decklist_id)
