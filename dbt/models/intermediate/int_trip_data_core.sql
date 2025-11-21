{{ config(materialized='view') }}

with trips as (

    select *
    from {{ ref('stg_trip_data') }}  -- later you can swap to a stg_core_trip_data if needed
    where
        total_amount > 0
        and trip_distance > 0
        and pickup_datetime < dropoff_datetime
        and passenger_count between 1 and 6

),

zones as (

    select *
    from {{ ref('stg_taxi_zone_lookup') }}

),

joined as (

    select
        t.*,
        pz.zone    as pickup_zone,
        pz.borough as pickup_borough,
        dz.zone    as dropoff_zone,
        dz.borough as dropoff_borough
    from trips t
    left join zones pz
        on t.pickup_location_id  = pz.location_id
    left join zones dz
        on t.dropoff_location_id = dz.location_id

)

select *
from joined
