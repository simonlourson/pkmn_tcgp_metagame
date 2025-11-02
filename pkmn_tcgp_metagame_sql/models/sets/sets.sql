{{ config(materialized='table') }}

select *
from {{ source('raw', 'sets') }}
