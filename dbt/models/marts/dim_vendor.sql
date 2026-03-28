{{ config(materialized='view') }}

with snapshot as (

    select *
    from {{ ref('snap_vendor') }}

),

current_vendors as (

    select
        vendor_id,
        vendor_name,
        status,
        dbt_valid_from      as valid_from,
        dbt_updated_at      as last_updated,

        -- Derive is_current from dbt_valid_to
        case 
            when dbt_valid_to is null then true 
            else false 
        end                 as is_current

    from snapshot
    where dbt_valid_to is null

)

select *
from current_vendors
