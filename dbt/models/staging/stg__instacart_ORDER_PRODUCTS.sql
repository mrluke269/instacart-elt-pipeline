{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id']
    )
}}

with source as (
    select * from {{ source('instacart_raw', 'ORDER_PRODUCTS') }}
),

renamed as (
    select
        order_id,
        product_id,
        add_to_cart_order,
        reordered,
        load_at
    from source
    {% if is_incremental() %}
    where load_at > (select max(load_at) from {{ this }})
    {% endif %}
)

select * from renamed