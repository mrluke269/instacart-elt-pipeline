{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('instacart_raw', 'DEPARTMENTS') }}
),

renamed as (
    select
        department_id,
        department
    from source
)

select * from renamed