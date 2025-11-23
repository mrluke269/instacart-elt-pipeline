{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with source as (
        select * from {{ source('instacart_raw', 'ORDERS') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ORDER_ID") }},
        {{ adapter.quote("USER_ID") }},
        {{ adapter.quote("EVAL_SET") }},
        {{ adapter.quote("ORDER_NUMBER") }},
        {{ adapter.quote("ORDER_DOW") }},
        {{ adapter.quote("ORDER_HOUR_OF_DAY") }},
        {{ adapter.quote("DAYS_SINCE_PRIOR_ORDER") }},
        {{ adapter.quote("ORDER_DATE") }},
        {{ adapter.quote("LOAD_AT") }}

      from source
      {% if is_incremental() %}
      where load_at > (select max(load_at) from {{ this }})
      {% endif %}
  )
  select * from renamed
    