{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with source as (
    select *
    from {{ ref('stg__instacart_ORDERS') }} o
),

renamed as (
    select
        *
    from source
    {% if is_incremental() %}
    where load_at > (select max(load_at) from {{ this }})
    {% endif %}
)

select * from renamed