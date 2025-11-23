{{ config(
    materialized='table'
) }}
with source as (
        select * from {{ source('instacart_raw', 'PRODUCTS') }}
  ),
  renamed as (
      select
          {{ adapter.quote("PRODUCT_ID") }},
        {{ adapter.quote("PRODUCT_NAME") }},
        {{ adapter.quote("AISLE_ID") }},
        {{ adapter.quote("DEPARTMENT_ID") }}

      from source
  )
  select * from renamed
    