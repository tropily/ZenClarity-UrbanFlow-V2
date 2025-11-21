{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('int_trip_data_core') }}
)

select
    -- grain / natural keys
    cab_type,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,

    -- simple date keys for later dims
    cast(pickup_datetime  as date) as pickup_date,
    cast(dropoff_datetime as date) as dropoff_date,

    -- core measures
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,

    -- handy denormalized attributes
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,

    -- lineage
    ingestion_ts
from src
