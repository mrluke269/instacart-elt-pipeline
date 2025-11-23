{{ config(
    materialized='table'
) }}

with source as (
    select 
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        a.AISLE,
        d.DEPARTMENT
    from {{ ref('stg__instacart_PRODUCTS') }} p
    join {{ ref('stg__instacart_AISLES') }} a 
        on p.AISLE_ID = a.AISLE_ID
    join {{ ref('stg__instacart_DEPARTMENTS') }} d 
        on p.DEPARTMENT_ID = d.DEPARTMENT_ID
),

renamed as (
    select
        {{ adapter.quote("PRODUCT_ID") }} as product_id,
        {{ adapter.quote("PRODUCT_NAME") }} as product_name,
        AISLE as aisle_name,
        DEPARTMENT as department_name
    from source
)

select * from renamed