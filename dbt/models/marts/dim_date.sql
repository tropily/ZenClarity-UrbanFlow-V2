

-- ============================================================
-- dim_date.sql
-- UrbanFlow V2 — Calendar date dimension
-- Marts (Gold) layer — date spine for time-based analytics
-- Source : generated calendar sequence (no upstream dependency)
-- Updated: April 2026
--  > April 2026 — Redshift portability fix
--    Snowflake: TABLE(GENERATOR()) + SEQ4() + DATEADD
--    Redshift:  UNION ALL row generator + dateadd spine
--    Spark/PG:  GENERATE_SERIES
-- ============================================================

{{ config(materialized='table') }}

with calendar as (

    {% if target.type == 'snowflake' %}
    select
        dateadd(day, seq4(), to_date('2020-01-01')) as date_key
    from table(generator(rowcount => 365 * 10))
    where date_key <= to_date('2030-12-31')

    {% elif target.type == 'redshift' %}
    select dateadd(day, n, '2020-01-01'::date) as date_key
    from (
        select row_number() over (order by 1) - 1 as n
        from (
            select 1 as x union all select 1 union all select 1 union all
            select 1 union all select 1 union all select 1 union all
            select 1 union all select 1 union all select 1 union all select 1
        ) a
        cross join (
            select 1 as x union all select 1 union all select 1 union all
            select 1 union all select 1 union all select 1 union all
            select 1 union all select 1 union all select 1 union all select 1
        ) b
        cross join (
            select 1 as x union all select 1 union all select 1 union all
            select 1 union all select 1 union all select 1 union all
            select 1 union all select 1 union all select 1 union all select 1
        ) c
        cross join (
            select 1 as x union all select 1 union all
            select 1 union all select 1
        ) d
    ) rows
    where dateadd(day, n, '2020-01-01'::date) <= '2030-12-31'::date

    {% else %}
    select
        ('2020-01-01'::date + (n || ' days')::interval)::date as date_key
    from generate_series(0, 3652) as t(n)
    where ('2020-01-01'::date + (n || ' days')::interval)::date <= '2030-12-31'
    {% endif %}

)

select
    date_key,                                           -- PK for date dim
    extract(year    from date_key) as year,
    extract(month   from date_key) as month,
    extract(day     from date_key) as day_of_month,
    to_char(date_key, 'YYYY-MM-DD') as date_str,
    to_char(date_key, 'DY')         as day_name,
    extract(dow     from date_key)  as day_of_week,
    extract(week    from date_key)  as week_of_year,
    extract(quarter from date_key)  as quarter
from calendar
order by date_key