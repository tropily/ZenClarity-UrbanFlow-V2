{{ config(materialized='view') }}

with base as (

    select *
    from {{ ref('stg_trip_data') }}

),

quarantined as (

    select
        b.*,

        -- Array of failure reasons for this trip
        {% if target.type == 'snowflake' %}
        array_construct_compact(
            iff(total_amount <= 0,                        'NON_POSITIVE_FARE',       null),
            iff(trip_distance <= 0,                       'NON_POSITIVE_DISTANCE',   null),
            iff(pickup_datetime >= dropoff_datetime,      'INVALID_DATETIME_ORDER',  null),
            iff(not (passenger_count between 1 and 6),   'INVALID_PASSENGER_COUNT', null)
        ) as dq_failure_reasons

        {% elif target.type == 'redshift' %}
        TRIM(',' FROM
            CASE WHEN total_amount <= 0
                 THEN 'NON_POSITIVE_FARE,' ELSE '' END
            || CASE WHEN trip_distance <= 0
                 THEN 'NON_POSITIVE_DISTANCE,' ELSE '' END
            || CASE WHEN pickup_datetime >= dropoff_datetime
                 THEN 'INVALID_DATETIME_ORDER,' ELSE '' END
            || CASE WHEN NOT (passenger_count BETWEEN 1 AND 6)
                 THEN 'INVALID_PASSENGER_COUNT,' ELSE '' END
        ) as dq_failure_reasons

        {% else %}
        NULL as dq_failure_reasons
        {% endif %}

    from base b
    where
        total_amount <= 0
        or trip_distance <= 0
        or pickup_datetime >= dropoff_datetime
        or not (passenger_count between 1 and 6)

)

select *
from quarantined
EOF