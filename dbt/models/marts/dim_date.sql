{{ config(materialized='table') }}

with calendar as (

    -- Adjust the start/end dates to your project horizon
    -- e.g. 2015-01-01 to 2030-12-31
    select
        dateadd(day, seq4(), to_date('2020-01-01')) as date_key
    from table(generator(rowcount => 365 * 10))  -- ~20 years

    where date_key <= to_date('2030-12-31')

)

select
    date_key,                               -- PK for date dim
    extract(year  from date_key) as year,
    extract(month from date_key) as month,
    extract(day   from date_key) as day_of_month,
    to_char(date_key, 'YYYY-MM-DD') as date_str,
    to_char(date_key, 'DY')         as day_name,
    extract(dow   from date_key)    as day_of_week,
    extract(week  from date_key)    as week_of_year,
    extract(quarter from date_key)  as quarter
from calendar
order by date_key
