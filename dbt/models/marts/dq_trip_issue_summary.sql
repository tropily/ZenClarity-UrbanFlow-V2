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

        /* array of failure reasons, e.g.:
            ['NON_POSITIVE_FARE','ZERO_DISTANCE']
        */
        dq_failure_reasons
    from {{ ref('int_trip_data_quarantine') }}

),

-- STEP 2: Derive load_date & explode each failure reason
exploded as (

    select
        q.vendor_id,
        q.cab_type,
        q.pickup_datetime,
        q.dropoff_datetime,
        q.total_amount,

        q.ingestion_ts,
        cast(q.ingestion_ts as date) as load_date,

        f.value::string as failure_reason
    from quarantined q,
         lateral flatten(input => q.dq_failure_reasons) f

),

-- STEP 3: Aggregate DQ issues for stewarding & monitoring
aggregated as (

    select
        load_date,
        failure_reason,

        count(*)          as trip_count,
        sum(total_amount) as total_amount_flagged,

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
