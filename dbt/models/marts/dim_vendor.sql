
-- ============================================================
-- dim_vendor.sql
-- UrbanFlow V2 — Vendor dimension (current state)
-- Marts (Gold) layer — SCD Type 2 current snapshot
-- Source : snap_vendor (dbt snapshot)
-- Updated: April 2026
--  > April 2026 — Redshift portability fix
--    Renamed CTE 'snapshot' → 'vendor_snapshot'
--    'snapshot' is a reserved word in Redshift
-- ============================================================

{{ config(materialized='view') }}

with vendor_snapshot as (

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

    from vendor_snapshot
    where dbt_valid_to is null

)

select *
from current_vendors