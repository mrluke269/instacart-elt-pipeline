{{
    config(
        materialized='table'
    )
}}

with source as (
        select * from {{ source('instacart_raw', 'AISLES') }}
  ),
  renamed as (
      select
          {{ adapter.quote("AISLE_ID") }},
        {{ adapter.quote("AISLE") }}

      from source
  )
  select * from renamed
    