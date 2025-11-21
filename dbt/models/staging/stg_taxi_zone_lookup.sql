{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('nyc_taxi', 'taxi_zone_lookup') }}

),

cleaned as (

    select
        -- Normalize ID naming
        cast(locationid as integer)           as location_id,

        -- Normalize text fields
        lower(trim(borough))                  as borough,
        lower(trim(zone))                     as zone,
        lower(trim(service_zone))             as service_zone


    from source

)

select *
from cleaned
