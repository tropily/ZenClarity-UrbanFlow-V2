# 03 — Benchmark Queries
## ZenClarity UrbanFlow V2 — Snowflake vs Redshift Benchmark Series

---

## Overview
Five queries run against the mart layer on both engines.
All queries are identical SQL — no engine-specific modifications.
Results validated for consistency across both engines before timing.

---

## Setup

### Snowflake
- Tool: DBeaver
- Database: NYC_TAXI_DEV
- Warehouse: COMPUTE_WH (X-Small)
- Schema context: MART_NYC_TAXI

### Redshift
- Tool: DBeaver
- Database: dev
- Workgroup: teo-nyc-workgroup (8 RPU)
- Schema context: mart_nyc_taxi

---

## How to Clear Cache Between Cold/Warm Runs

**Snowflake — suspend and resume warehouse:**
```sql
ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER WAREHOUSE COMPUTE_WH RESUME;
```

**Redshift — let workgroup idle:**
Wait 2-3 minutes between cold runs.
Serverless auto-pauses after idle period — next query is a cold start.

---

## Query 1 — Full Mart Aggregate Scan

**What it tests:** Full table scan performance, GROUP BY on 41M records,
columnar compression efficiency, unfiltered aggregate throughput.

```sql
SELECT
    cab_type,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(trip_distance), 2)    AS avg_distance
FROM mart_nyc_taxi.fact_trip
GROUP BY cab_type
ORDER BY trip_count DESC;
```

Expected rows: 4 (yellow, green)

---

## Query 2 — Filtered Date Range Scan

**What it tests:** Filter pushdown efficiency, partition pruning on
pickup_date_key, single month scan against a 41M record table.

```sql
SELECT
    pickup_date_key,
    cab_type,
    COUNT(*)                        AS daily_trips,
    ROUND(SUM(total_amount), 2)     AS daily_revenue
FROM mart_nyc_taxi.fact_trip
WHERE pickup_date_key BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY pickup_date_key, cab_type
ORDER BY pickup_date_key, cab_type;
```

Expected rows: ~124 (31 days × 4 cab types)

---

## Query 3 — Join Query (Fact + Dimension)

**What it tests:** Join performance between fact and dimension table,
zone enrichment lookup efficiency, multi-column GROUP BY.

```sql
SELECT
    z.borough,
    f.cab_type,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(f.tip_amount), 2)     AS avg_tip,
    ROUND(AVG(f.trip_distance), 2)  AS avg_distance,
    ROUND(SUM(f.total_amount), 2)   AS total_revenue
FROM mart_nyc_taxi.fact_trip f
JOIN mart_nyc_taxi.dim_taxi_zone z
    ON f.pickup_location_id = z.location_id
GROUP BY z.borough, f.cab_type
ORDER BY total_revenue DESC;
```

Expected rows: ~8 (5 boroughs × cab types with trips)

---

## Query 4 — DQ Layer Summary

**What it tests:** View resolution performance through the DQ layer,
aggregation on a filtered subset, intermediate layer access speed.

```sql
SELECT
    failure_reason,
    COUNT(*)                            AS trip_count,
    ROUND(SUM(total_amount_flagged), 2) AS total_flagged
FROM mart_nyc_taxi.dq_trip_issue_summary
GROUP BY failure_reason
ORDER BY trip_count DESC;
```

Expected rows: 4 (one per DQ failure reason)

---

## Query 5 — Time of Day Analysis

**What it tests:** Derived metric query performance, CASE WHEN bucket
evaluation on a large table, multi-dimension GROUP BY throughput.

```sql
SELECT
    time_of_day,
    cab_type,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(tip_pct) * 100, 2)   AS avg_tip_pct
FROM mart_nyc_taxi.fact_trip
GROUP BY time_of_day, cab_type
ORDER BY time_of_day, trip_count DESC;
```

Expected rows: ~20 (5 time buckets × 4 cab types)

---

## Recording Results

DBeaver shows execution time at the bottom of the results panel
after each run in the format: `X rows fetched in Xs Yms`

Record:
- Cold run time (after cache cleared)
- Warm run time (immediate re-run, no changes)
- Row count (validate consistency across engines)

Results documented in: `04_benchmark_results.md`
