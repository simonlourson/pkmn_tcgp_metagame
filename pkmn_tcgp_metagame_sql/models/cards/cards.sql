{{ config(materialized='table') }}

with cards_with_duplicate_names as (
  select
    c.set_code,
    c.card_name
  from raw.cards as c
  where not c.is_promo
  group by c.set_code, c.card_name
  having count(*) > 1
)

select
  en.card_url,
  en.set_code,
  en.card_number,
  {{ env_var('DBT_LOCALE') }}.card_name,
  en.card_name as card_name_en,
  fr.card_name as card_name_fr,
  {{ env_var('DBT_LOCALE') }}.card_name || case when cwdn.card_name is not null then ' (' || en.card_number || ')' else '' end || ' (' || en.set_code || ')' as card_name_with_set,
  en.card_type,
  en.card_subtype,
  en.card_stage
from {{ source('raw', 'cards') }} as en
inner join {{ source('raw', 'translations') }} as fr on en.set_code = fr.set_code and en.card_number = fr.card_number
left outer join cards_with_duplicate_names as cwdn on en.set_code = cwdn.set_code and en.card_name = cwdn.card_name
