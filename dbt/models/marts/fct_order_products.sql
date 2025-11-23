{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id']
) }}

with source as (
    select *
    from {{ ref('stg__instacart_ORDER_PRODUCTS') }} op
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