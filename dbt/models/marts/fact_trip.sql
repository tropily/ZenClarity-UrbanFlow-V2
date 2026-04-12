
-- ============================================================
-- fact_trip.sql
-- UrbanFlow V2 — Trip fact table
-- Marts (Gold) layer — pre-computed metrics, star schema ready
-- Source : int_trip_data_core (intermediate-silver layer)
-- Updated: April 2026
--  > Added Time of day bucket — demand pattern analysis
--  > Added Airport Flag
--  > Added tip %
--  > April 2026 — Redshift portability fix (HOUR → EXTRACT)
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    on_schema_change='sync_all_columns'
) }}

with core as (

    select *
    from {{ ref('int_trip_data_core') }}

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
        {{ safe_divide('tip_amount', 'total_amount') }}  as tip_pct,

        -- Time of day bucket — demand pattern analysis
        case
            when {% if target.type == 'redshift' %}
                 extract(hour from pickup_datetime)
                 {% else %}
                 hour(pickup_datetime)
                 {% endif %} between 6  and 9  then 'morning'
            when {% if target.type == 'redshift' %}
                 extract(hour from pickup_datetime)
                 {% else %}
                 hour(pickup_datetime)
                 {% endif %} between 10 and 15 then 'afternoon'
            when {% if target.type == 'redshift' %}
                 extract(hour from pickup_datetime)
                 {% else %}
                 hour(pickup_datetime)
                 {% endif %} between 16 and 19 then 'evening'
            when {% if target.type == 'redshift' %}
                 extract(hour from pickup_datetime)
                 {% else %}
                 hour(pickup_datetime)
                 {% endif %} between 20 and 23 then 'night'
            else 'late_night'
        end                                              as time_of_day,

        -- Airport trip flag
        {{ is_airport_trip('pickup_location_id') }}      as is_airport_pickup,
        {{ is_airport_trip('dropoff_location_id') }}     as is_airport_dropoff,

        -- ── Lineage ──
        ingestion_ts

    from core

)

select *
from final
