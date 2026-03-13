-- ============================================================
-- int_trip_data_core.sql
-- UrbanFlow V2 — Intermediate core model for NYC Taxi trips
-- Silver layer — quality filtered, zone enriched, keyed
-- Source : stg_trip_data (bronze view)
-- Updated: March 2026
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
) }}

with trips as (

    select *
    from {{ ref('stg_trip_data') }}

    -- Incremental filter — only process new records on each run
    -- First run  → full load (is_incremental() = false)
    -- Delta runs → only records newer than last ingestion_ts
    {% if is_incremental() %}
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
    {% endif %}

),

quality_filtered as (

    -- SCD Type 1 — latest version wins on MERGE
    -- Bad records routed to int_trip_data_quarantine (view)
    select *
    from trips
    where
        total_amount      > 0
        and trip_distance > 0
        and pickup_datetime < dropoff_datetime
        and passenger_count between 1 and 6

),

zones as (

    select *
    from {{ ref('stg_taxi_zone_lookup') }}

),

enriched as (

    select
        -- surrogate key — generated from immutable business columns
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'cab_type',
            'pickup_datetime',
            'pickup_location_id',
            'dropoff_location_id'
        ]) }}                                   as trip_id,

        -- core trip identifiers
        t.vendor_id,
        t.cab_type,

        -- timestamps
        t.pickup_datetime,
        t.dropoff_datetime,

        -- derived metric — trip duration
        datediff('minute',
            t.pickup_datetime,
            t.dropoff_datetime)                 as trip_duration_minutes,

        -- flags / codes
        t.store_and_fwd_flag,
        t.rate_code_id,

        -- locations — IDs + readable names
        t.pickup_location_id,
        pz.zone                                 as pickup_zone,
        pz.borough                              as pickup_borough,
        t.dropoff_location_id,
        dz.zone                                 as dropoff_zone,
        dz.borough                              as dropoff_borough,

        -- trip attributes
        t.passenger_count,
        t.trip_distance,

        -- monetary fields
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.ehail_fee,
        t.improvement_surcharge,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee,

        -- payment / trip type
        t.payment_type,
        t.trip_type,

        -- pipeline metadata — lineage + audit
        t.ingestion_ts,
        t.batch_id,
        t.source_file,
        t.migration_batch_id,
        t.engine_type

    from quality_filtered t
    left join zones pz
        on t.pickup_location_id  = pz.location_id
    left join zones dz
        on t.dropoff_location_id = dz.location_id

),

deduped as (

    -- Dedup — remove duplicate submissions from source system
    -- Same trip submitted multiple times with different metrics
    -- Duplicates captured in int_trip_data_dq_duplicates (view)
    -- ROW_NUMBER keeps latest submission — SCD Type 1 latest wins
    select *
    from (
        select *,
            row_number() over (
                partition by trip_id
                order by dropoff_datetime desc
            ) as rn
        from enriched
    )
    where rn = 1

)

select * exclude(rn)
from deduped
