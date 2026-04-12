-- ============================================================
-- stg_taxi_zone_lookup.sql
-- UrbanFlow V2 — Staging model for NYC Taxi zone lookup
-- Source : nyc_taxi_wh.taxi_zone_lookup (Iceberg via Glue catalog)
-- Updated: April 2026
-- ============================================================

{{ config(
    materialized = 'table' if target.type == 'redshift' else 'view'
) }}

with source as (

    select *
    from {{ source('nyc_taxi', 'taxi_zone_lookup') }}

),

cleaned as (

    select
        -- Normalize ID naming
        cast(locationid as integer)           as location_id,

        -- Normalize text fields
        lower(trim(borough))                  as borough,
        lower(trim(zone))                     as zone,
        lower(trim(service_zone))             as service_zone

    from source

)

select *
from cleaned