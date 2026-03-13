
-- ============================================================
-- fact_trip.sql
-- UrbanFlow V2 — Trip fact table
-- Marts (Gold) layer — pre-computed metrics, star schema ready
-- Source : int_trip_data_core (intermediate-silver layer)
-- Updated: March 2026
--  > Added Time of day bucket — demand pattern analysis
--  >  Added Airport Flag 
--  > Added tip %
--UPDATED 03-2026
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
) }}

with core as (

    select *
    from {{ ref('int_trip_data_core') }}

    -- Incremental filter — arrival time watermark
    -- Handles late arriving + historical reloads correctly
    {% if is_incremental() %}
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
    {% endif %}

),

final as (

    select
        -- ── Surrogate key ──
        trip_id,

        -- ── Trip identifiers ──
        vendor_id,
        cab_type,

        -- ── Timestamps ──
        pickup_datetime,
        dropoff_datetime,

        -- ── Date keys → FK to dim_date ──
        cast(pickup_datetime  as date) as pickup_date_key,
        cast(dropoff_datetime as date) as dropoff_date_key,

        -- ── Duration ──
        trip_duration_minutes,

        -- ── Location IDs → FK to dim_taxi_zone ──
        pickup_location_id,
        dropoff_location_id,

        -- ── Denormalized zone names (pre-joined in core) ──
        pickup_zone,
        pickup_borough,
        dropoff_zone,
        dropoff_borough,

        -- ── Trip attributes ──
        store_and_fwd_flag,
        rate_code_id,
        passenger_count,
        trip_distance,
        payment_type,
        trip_type,

        -- ── Monetary fields ──
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

        -- ── Derived metrics ──
        -- Tip percentage — tipping behavior by zone/cab/hour
        round(
            tip_amount / nullif(fare_amount, 0) * 100,
        2)                                  as tip_pct,

        -- Time of day bucket — demand pattern analysis
        case
            when hour(pickup_datetime) between 6  and 9  then 'morning'
            when hour(pickup_datetime) between 10 and 15 then 'afternoon'
            when hour(pickup_datetime) between 16 and 19 then 'evening'
            when hour(pickup_datetime) between 20 and 23 then 'night'
            else 'late_night'
        end                                 as time_of_day,

        -- Airport trip flag — high value segment
        -- Location IDs: 1=EWR, 132=JFK, 138=LGA
        case
            when pickup_location_id  in (1, 132, 138) then true
            when dropoff_location_id in (1, 132, 138) then true
            else false
        end                                 as is_airport_trip,

        -- ── Lineage ──
        ingestion_ts
        -- metadata columns dropped:
        -- batch_id, source_file, migration_batch_id, engine_type

    from core

)

select *
from final