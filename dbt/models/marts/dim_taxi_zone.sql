{{ config(materialized='view') }}

with src as (
    select *
    from {{ ref('stg_taxi_zone_lookup') }}
)

select
    location_id,     -- primary key
    borough,
    zone,
    service_zone
from src
