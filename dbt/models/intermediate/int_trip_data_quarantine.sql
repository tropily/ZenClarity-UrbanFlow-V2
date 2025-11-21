{{ config(materialized='view') }}

with base as (

    select *
    from {{ ref('stg_trip_data') }}

),

quarantined as (

    select
        b.*,

        -- Array of failure reasons for this trip
        array_construct_compact(
            iff(total_amount <= 0, 'NON_POSITIVE_FARE', null),
            iff(trip_distance <= 0, 'NON_POSITIVE_DISTANCE', null),
            iff(pickup_datetime >= dropoff_datetime, 'INVALID_DATETIME_ORDER', null),
            iff(not (passenger_count between 1 and 6), 'INVALID_PASSENGER_COUNT', null)
        ) as dq_failure_reasons

    from base b
    where
        total_amount <= 0
        or trip_distance <= 0
        or pickup_datetime >= dropoff_datetime
        or not (passenger_count between 1 and 6)

)

select *
from quarantined
