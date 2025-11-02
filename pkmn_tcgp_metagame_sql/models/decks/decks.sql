{{ config(materialized='table') }}

with decks as (
  select
    dp.decklist_id,
    string_agg(
      c.card_url::varchar, '#'
      order by c.card_url
    ) as deck_id,
    string_agg(
      c.card_name_with_set, ', '
      order by c.card_url
    ) as deck_name
  from {{ ref("decklists_pruned") }} as dp
  inner join {{ ref("cards") }} as c on dp.card_url = c.card_url
  group by dp.decklist_id
)

select distinct
  deck_id,
  deck_name
from decks
