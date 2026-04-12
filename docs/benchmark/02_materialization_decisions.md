# 02 — Materialization Decisions
## ZenClarity UrbanFlow V2 — Snowflake vs Redshift Benchmark Series

---

## Overview
This document explains why the staging layer materializes differently across
engines, how engine-specific syntax differences are handled, and why a single
dbt codebase serves both targets without splitting into separate files.

---

## The Core Problem

Redshift Spectrum does not support views over external tables. On Snowflake,
staging models are zero-cost views that resolve directly to S3 Iceberg at
query time. On Redshift, the same pattern fails — Spectrum external tables
cannot be wrapped in a view.

The solution: engine-specific materialization via dbt Jinja conditionals.
No model was split into separate files. All differences are isolated to
config blocks and inline conditionals within the same SQL file.

---

## Materialization by Layer

| Layer | Model | Snowflake | Redshift | Reason |
|-------|-------|-----------|----------|--------|
| Staging | stg_trip_data | view | incremental table | Spectrum limitation |
| Staging | stg_taxi_zone_lookup | view | table | Spectrum limitation |
| Staging | stg_vendor | view | table (seed) | Seed required in Redshift |
| Intermediate | int_trip_data_core | incremental | incremental | Same |
| Intermediate | int_trip_data_quarantine | view | view | Same |
| Intermediate | int_trip_data_dq_duplicates | view | view | Same |
| Mart | fact_trip | incremental | incremental | Same |
| Mart | dim_date | table | table | Same |
| Mart | dim_taxi_zone | view | view | Same |
| Mart | dim_vendor | view | view | Same |
| Mart | dq_trip_issue_summary | view | view | Same |

---

## Incremental Boundary Difference

On Snowflake the incremental boundary is at the **intermediate layer**.
Staging is a zero-cost view — no data is stored at that layer.

On Redshift the incremental boundary is pushed to the **staging layer**
because Spectrum cannot be viewed. stg_trip_data uses an incremental
merge strategy so subsequent runs only process new records via an
ingestion_ts watermark filter — not a full 41M record scan every run.
**Impact on build time:**
- Snowflake staging adds ~0s per build (view — no data movement)
- Redshift staging adds ~6s on full build, ~5s on incremental run

**Impact on query performance:**
- None — benchmark queries hit the mart layer only
- Both engines query physical materialized tables at query time

---

## Engine-Specific Syntax Fixes

Six models required syntax changes for Redshift compatibility.
All changes use target.type Jinja conditionals — no files split.

---

### stg_trip_data — Materialization Config
```sql
{{ config(
    materialized         = 'incremental' if target.type == 'redshift' else 'view',
    unique_key           = ['vendor_id', 'cab_type', 'pickup_location_id',
                            'dropoff_location_id', 'pickup_datetime', 'ingestion_ts']
                            if target.type == 'redshift' else none,
    incremental_strategy = 'merge'          if target.type == 'redshift' else none,
    dist                 = 'ingestion_ts'   if target.type == 'redshift' else none,
    sort                 = ['ingestion_ts'] if target.type == 'redshift' else none
) }}
```
Why ingestion_ts as sort key: staging's primary workload is the incremental
filter `where ingestion_ts > max(ingestion_ts)`. Sort key matches the access pattern.

---

### int_trip_data_quarantine — Array Construction
Snowflake supports native arrays and IFF(). Redshift uses string concatenation.

```sql
{% if target.type == 'snowflake' %}
array_construct_compact(
    iff(total_amount <= 0, 'NON_POSITIVE_FARE', null),
    ...
) as dq_failure_reasons

{% elif target.type == 'redshift' %}
TRIM(',' FROM
    CASE WHEN total_amount <= 0 THEN 'NON_POSITIVE_FARE,' ELSE '' END
    || CASE WHEN trip_distance <= 0 THEN 'NON_POSITIVE_DISTANCE,' ELSE '' END
    ...
) as dq_failure_reasons
{% endif %}
```

---

### dim_date — Row Generation
Snowflake uses TABLE(GENERATOR()) + SEQ4() to generate a date spine.
Redshift uses a CROSS JOIN UNION ALL pattern — no system table access required.

```sql
{% if target.type == 'snowflake' %}
select dateadd(day, seq4(), to_date('2020-01-01')) as date_key
from table(generator(rowcount => 365 * 10))

{% elif target.type == 'redshift' %}
select dateadd(day, n, '2020-01-01'::date) as date_key
from (
    select row_number() over (order by 1) - 1 as n
    from (select 1 as x union all ...) cross join (...)
)
{% endif %}
```

---

### fact_trip — Date Function
HOUR(timestamp) is Snowflake-only. Redshift uses EXTRACT.
Pre-computed in a CTE for clean CASE WHEN logic:

```sql
{% if target.type == 'redshift' %}
extract(hour from pickup_datetime)
{% else %}
hour(pickup_datetime)
{% endif %} as pickup_hour
```

---

### dim_vendor — Reserved Word
snapshot is a reserved word in Redshift. The CTE was renamed
vendor_snapshot — compatible with both engines, no logic change.

---

### dq_trip_issue_summary — Array Flattening
Snowflake uses LATERAL FLATTEN on a native array.
Redshift uses SPLIT_PART + UNION ALL on the CSV string produced
by the Redshift quarantine model — one block per possible failure reason.

```sql
{% if target.type == 'snowflake' %}
from quarantined q, lateral flatten(input => q.dq_failure_reasons) f

{% elif target.type == 'redshift' %}
select ... trim(split_part(dq_failure_reasons, ',', 1)) as failure_reason
from quarantined where trim(split_part(..., 1)) != ''
union all
select ... trim(split_part(dq_failure_reasons, ',', 2)) as failure_reason
...
{% endif %}
```

Note: the UNION ALL approach supports up to 4 failure reasons —
matching the 4 DQ checks in int_trip_data_quarantine.
Adding a 5th check requires a new UNION ALL block.

---

## Single Codebase Decision

All engine differences are handled inline — no model was split into
separate Snowflake and Redshift versions.

**Why single file:**
- Prevents logic drift — one place to update business rules
- Forces explicit documentation of engine differences via comments
- Consistent with the portability story — one codebase, multiple targets
- Easier to maintain — changes apply to both engines simultaneously

**Where this breaks down:**
If engine differences grow significantly (5+ conditionals per model),
splitting becomes more readable. For this project the differences are
contained and well-documented — single file is the right call.

---

## dbt Build Performance Impact

| Metric | Snowflake XS | Redshift 8 RPU |
|--------|-------------|----------------|
| Full build time | 14.65s | 36.93s |
| Staging build cost | ~0s (views) | ~6s (incremental tables) |
| Incremental build time | ~2s | ~5s |
| Tests (52 total) | ~0.2s each | ~1.1s each |

Snowflake builds 2.5x faster primarily because staging is zero-cost views.
Test execution is also faster on Snowflake due to result caching behavior.
