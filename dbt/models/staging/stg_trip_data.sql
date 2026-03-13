-- ============================================================
-- stg_trip_data.sql
-- UrbanFlow V2 — Staging model for NYC Taxi trip data
-- Source : nyc_taxi_wh.trip_data (Iceberg via Glue catalog)
-- Updated: March 2026
-- ============================================================

{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('nyc_taxi', 'trip_data') }}

),

cleaned as (

    select
        -- core identifiers / attributes
        cast(vendorid as integer)              as vendor_id,
        lower(trim(cab_type))                  as cab_type,

        -- timestamps
        cast(pickup_datetime  as timestamp)    as pickup_datetime,
        cast(dropoff_datetime as timestamp)    as dropoff_datetime,

        -- flags / codes
        lower(trim(store_and_fwd_flag))        as store_and_fwd_flag,
        cast(ratecodeid as integer)            as rate_code_id,

        -- locations
        cast(pulocationid as integer)          as pickup_location_id,
        cast(dolocationid as integer)          as dropoff_location_id,

        -- trip attributes
        cast(passenger_count as integer)       as passenger_count,
        cast(trip_distance   as double)        as trip_distance,

        -- monetary fields
        cast(fare_amount           as double)  as fare_amount,
        cast(extra                 as double)  as extra,
        cast(mta_tax               as double)  as mta_tax,
        cast(tip_amount            as double)  as tip_amount,
        cast(tolls_amount          as double)  as tolls_amount,
        cast(ehail_fee             as double)  as ehail_fee,
        cast(improvement_surcharge as double)  as improvement_surcharge,
        cast(total_amount          as double)  as total_amount,
        cast(congestion_surcharge  as double)  as congestion_surcharge,
        cast(airport_fee           as double)  as airport_fee,

        -- payment / trip type
        cast(payment_type as integer)          as payment_type,
        cast(trip_type    as integer)          as trip_type,

        -- pipeline metadata — lineage + audit
        cast(ingestion_ts       as timestamp)  as ingestion_ts,
        cast(batch_id           as varchar)    as batch_id,
        cast(source_file        as varchar)    as source_file,
        cast(migration_batch_id as varchar)    as migration_batch_id,
        cast(engine_type        as varchar)    as engine_type

    from source

)

select *
from cleaned