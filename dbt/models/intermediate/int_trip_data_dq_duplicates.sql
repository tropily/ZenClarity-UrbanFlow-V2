{{ config(materialized='view') }}

-- ============================================================
-- int_trip_data_dq_duplicates.sql
-- UrbanFlow V2 — Duplicate trip detection view
-- Silver layer — DQ signal for upstream investigation
-- Source : stg_trip_data (bronze view)
--
-- Purpose:
--   Detects duplicate trip submissions from source system
--   where the same vendor/cab/pickup/dropoff combination
--   appears more than once with different metrics
--   (fare, distance, dropoff_datetime)
--
-- Behavior:
--   → self-healing view — duplicates disappear automatically
--     when upstream fixes source data
--   → does NOT source from core — reads raw staging
--     so all submissions are visible before dedup
--   → feeds upstream DQ investigation
--
-- Updated: March 2026
-- ============================================================

with base as (

    select *
    from {{ ref('stg_trip_data') }}

),

trip_counts as (

    select
        vendor_id,
        cab_type,
        pickup_datetime,
        pickup_location_id,
        dropoff_location_id,
        count(*)                    as submission_count,
        min(dropoff_datetime)       as earliest_dropoff,
        max(dropoff_datetime)       as latest_dropoff,
        min(total_amount)           as min_fare,
        max(total_amount)           as max_fare,
        min(trip_distance)          as min_distance,
        max(trip_distance)          as max_distance,
        min(ingestion_ts)           as first_seen,
        max(ingestion_ts)           as last_seen

    from base
    group by
        vendor_id,
        cab_type,
        pickup_datetime,
        pickup_location_id,
        dropoff_location_id
    having count(*) > 1            -- duplicates only

),

flagged as (

    select
        b.*,
        tc.submission_count,
        tc.earliest_dropoff,
        tc.latest_dropoff,
        tc.min_fare,
        tc.max_fare,
        tc.min_distance,
        tc.max_distance,
        tc.first_seen,
        tc.last_seen,

        -- Variance signals — helps upstream identify
        -- how different the duplicate submissions are
        tc.max_fare      - tc.min_fare       as fare_variance,
        tc.max_distance  - tc.min_distance   as distance_variance,
        datediff('minute',
            tc.earliest_dropoff,
            tc.latest_dropoff)               as dropoff_variance_minutes

    from base b
    inner join trip_counts tc
        on  b.vendor_id           = tc.vendor_id
        and b.cab_type            = tc.cab_type
        and b.pickup_datetime     = tc.pickup_datetime
        and b.pickup_location_id  = tc.pickup_location_id
        and b.dropoff_location_id = tc.dropoff_location_id

)

select *
from flagged
