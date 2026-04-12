
-- ============================================================
-- dq_trip_issue_summary.sql
-- UrbanFlow V2 — DQ issue summary by load date + failure reason
-- Marts (Gold) layer — data steward monitoring view
-- Source : int_trip_data_quarantine (intermediate-silver layer)
-- Updated: April 2026
--  > April 2026 — Redshift portability fix
--    Snowflake: LATERAL FLATTEN on array dq_failure_reasons
--    Redshift:  UNION ALL on SPLIT_PART of CSV string
-- ============================================================

{{ config(materialized='view') }}

-- STEP 1: Bring in the quarantined trips
with quarantined as (

    select
        vendor_id,
        cab_type,
        pickup_datetime,
        dropoff_datetime,
        total_amount,
        ingestion_ts,
        dq_failure_reasons
    from {{ ref('int_trip_data_quarantine') }}

),

-- STEP 2: Explode each failure reason
exploded as (

    {% if target.type == 'snowflake' %}
    select
        q.vendor_id,
        q.cab_type,
        q.pickup_datetime,
        q.dropoff_datetime,
        q.total_amount,
        q.ingestion_ts,
        cast(q.ingestion_ts as date)  as load_date,
        f.value::string               as failure_reason
    from quarantined q,
         lateral flatten(input => q.dq_failure_reasons) f

    {% elif target.type == 'redshift' %}
    -- Redshift: dq_failure_reasons is a CSV string
    -- UNION ALL on up to 4 possible failure reasons
    select
        vendor_id, cab_type, pickup_datetime, dropoff_datetime,
        total_amount, ingestion_ts,
        cast(ingestion_ts as date) as load_date,
        trim(split_part(dq_failure_reasons, ',', 1)) as failure_reason
    from quarantined
    where trim(split_part(dq_failure_reasons, ',', 1)) != ''

    union all

    select
        vendor_id, cab_type, pickup_datetime, dropoff_datetime,
        total_amount, ingestion_ts,
        cast(ingestion_ts as date) as load_date,
        trim(split_part(dq_failure_reasons, ',', 2)) as failure_reason
    from quarantined
    where trim(split_part(dq_failure_reasons, ',', 2)) != ''

    union all

    select
        vendor_id, cab_type, pickup_datetime, dropoff_datetime,
        total_amount, ingestion_ts,
        cast(ingestion_ts as date) as load_date,
        trim(split_part(dq_failure_reasons, ',', 3)) as failure_reason
    from quarantined
    where trim(split_part(dq_failure_reasons, ',', 3)) != ''

    union all

    select
        vendor_id, cab_type, pickup_datetime, dropoff_datetime,
        total_amount, ingestion_ts,
        cast(ingestion_ts as date) as load_date,
        trim(split_part(dq_failure_reasons, ',', 4)) as failure_reason
    from quarantined
    where trim(split_part(dq_failure_reasons, ',', 4)) != ''

    {% else %}
    select
        vendor_id, cab_type, pickup_datetime, dropoff_datetime,
        total_amount, ingestion_ts,
        cast(ingestion_ts as date) as load_date,
        trim(reason)               as failure_reason
    from quarantined,
         unnest(string_to_array(dq_failure_reasons, ',')) as reason
    {% endif %}

),

-- STEP 3: Aggregate DQ issues for stewarding & monitoring
aggregated as (

    select
        load_date,
        failure_reason,
        count(*)             as trip_count,
        sum(total_amount)    as total_amount_flagged,
        min(pickup_datetime) as first_seen_issue_at,
        max(pickup_datetime) as last_seen_issue_at
    from exploded
    group by
        load_date,
        failure_reason

)

-- FINAL
select *
from aggregated
order by load_date desc, failure_reason
