{{ config(materialized='table') }}

select
  set_code,
  deck_id,
  sum(nb_wins)::int as nb_wins,
  sum(nb_losses)::int as nb_losses,
  sum(nb_wins)::float / (sum(nb_wins) + sum(nb_losses)) as win_rate
from {{ ref("deck_against_deck_win_rate") }}
group by set_code, deck_id
