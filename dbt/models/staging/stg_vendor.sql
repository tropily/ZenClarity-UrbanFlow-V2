{{ config(materialized='view') }}

with source as (

    select *
    from {{ ref('vendor') }}

),

cleaned as (

    select
        -- Normalize ID naming
        cast(vendor_id as integer)           as vendor_id,

        -- Normalize text fields
        trim(vendor_name)                  as vendor_name,
        lower(trim(status))                       as status


    from source

)

select * 
from cleaned
