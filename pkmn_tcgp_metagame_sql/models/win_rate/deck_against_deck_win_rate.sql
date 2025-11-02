{{ config(materialized='table') }}

with one_way as (
  select
    t.set_code,
    da_winner.deck_id as winner_deck,
    da_loser.deck_id as loser_deck,
    count(*) as nb_matches
  from {{ ref("matches") }} as m
  inner join {{ ref("tournaments") }} as t on m.tournament_id = t.tournament_id
  inner join
    {{ ref("decklists_aggregated") }} as da_winner
    on (m.winner_decklist_id = da_winner.decklist_id)
  inner join
    {{ ref("decklists_aggregated") }} as da_loser
    on (m.loser_decklist_id = da_loser.decklist_id)
  group by t.set_code, da_winner.deck_id, da_loser.deck_id
),

both_ways as (
  select
    set_code,
    winner_deck as deck_id,
    loser_deck as against_deck_id,
    nb_matches as nb_wins,
    0 as nb_losses
  from one_way
  union
  select
    set_code,
    loser_deck as deck_id,
    winner_deck as against_deck_id,
    0 as nb_wins,
    nb_matches as nb_losses
  from one_way
)

select
  set_code,
  deck_id,
  against_deck_id,
  sum(nb_wins)::int as nb_wins,
  sum(nb_losses)::int as nb_losses,
  sum(nb_wins)::float / (sum(nb_wins) + sum(nb_losses)) as win_rate
from both_ways
group by set_code, deck_id, against_deck_id
