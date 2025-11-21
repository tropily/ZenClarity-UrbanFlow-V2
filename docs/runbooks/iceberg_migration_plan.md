# Iceberg Migration Plan — 2024 Backfill (ZenClarity-UrbanFlow V2)

**Owner:** Nguyen Le  
**Scope:** Migrate 2024 *yellow* + *green* taxi trip data from processed zone → Iceberg warehouse staging table (`trip_data_v2_stage`), validate, then promote to production (`trip_data`).  
**Purpose:** Establish a safe, deterministic, auditable migration workflow for Iceberg-based lakehouse tables.

---

# 1. Architecture Overview

```
processed (Parquet, partitioned)
    ↓
Spark Backfill Job (schema normalize + partition filter + ingestion_ts)
    ↓
Iceberg Staging Table (trip_data_v2_stage)
    ↓
Validation Suite
    ↓
Promotion → Production Iceberg Table (trip_data)
```

---

# 2. Source → Destination Definition

## 2.1 Source Dataset

We are ingesting from the **processed zone**.

### Processed S3 Layout (partitioned)

```
s3://teo-nyc-taxi/processed/trip_data/cab_type=yellow/year=2024/month=5/day=16/part-xxxx.snappy.parquet
s3://teo-nyc-taxi/processed/trip_data/cab_type=green/year=2024/month=8/day=20/part-xxxx.snappy.parquet
```

### Source Table Schema (Glue External Table)

- vendorid  
- pickup_datetime  
- dropoff_datetime  
- store_and_fwd_flag  
- ratecodeid  
- pulocationid  
- dolocationid  
- passenger_count  
- trip_distance  
- fare_amount  
- extra  
- mta_tax  
- tip_amount  
- tolls_amount  
- ehail_fee  
- improvement_surcharge  
- total_amount  
- payment_type  
- trip_type  
- congestion_surcharge  
- airport_fee  
- partition columns: **cab_type, year, month, day**

Partition columns become normal DataFrame columns on read.

---

## 2.2 Destination: Production Iceberg Table

```
nyc_taxi_wh.trip_data
LOCATION: s3://teo-nyc-taxi/warehouse/nyc_taxi_wh/trip_data
PARTITIONED BY: day(pickup_datetime)
```

### Schema

| Column | Type |
|--------|------|
| vendorid | int |
| cab_type | string |
| pickup_datetime | timestamp |
| dropoff_datetime | timestamp |
| store_and_fwd_flag | string |
| ratecodeid | bigint |
| pulocationid | int |
| dolocationid | int |
| passenger_count | bigint |
| trip_distance | double |
| fare_amount | double |
| extra | double |
| mta_tax | double |
| tip_amount | double |
| tolls_amount | double |
| ehail_fee | double |
| improvement_surcharge | double |
| total_amount | double |
| payment_type | bigint |
| trip_type | bigint |
| congestion_surcharge | double |
| airport_fee | double |
| ingestion_ts | timestamp |

---

# 3. Safe V2 Approach: Iceberg Staging Table

To avoid corrupting production during rework/testing:

### Staging Table

```
nyc_taxi_wh.trip_data_v2_stage
LOCATION: s3://teo-nyc-taxi/warehouse/nyc_taxi_wh/trip_data_v2_stage
```

Created via:

```sql
CREATE TABLE IF NOT EXISTS nyc_taxi_wh.trip_data_v2_stage
LIKE nyc_taxi_wh.trip_data
LOCATION 's3://teo-nyc-taxi/warehouse/nyc_taxi_wh/trip_data_v2_stage';
```

All 2024 backfill writes **only** to this table.

---

# 4. Backfill Logic

## 4.1 Data Filtering Rules

- Only **2024** records  
- Only **yellow** + **green**  
- Ignore `fhv` and `hvfhs` (reserved for V3)

---

## 4.2 Schema Normalization Rules

Cast all fields to Iceberg schema:

```
vendorid int
cab_type string
pickup_datetime timestamp
dropoff_datetime timestamp
store_and_fwd_flag string
ratecodeid bigint
pulocationid int
dolocationid int
passenger_count bigint
trip_distance double
fare_amount double
extra double
mta_tax double
tip_amount double
tolls_amount double
ehail_fee double (NULL if missing)
improvement_surcharge double
total_amount double
payment_type bigint
trip_type bigint (NULL if missing)
congestion_surcharge double (NULL if missing)
airport_fee double (NULL if missing)
ingestion_ts current_timestamp()
```

---

## 4.3 Spark Backfill Job

**File:** `scripts/batch/iceberg_backfill_2024.py`  
Supports:

- sample mode  
- month/day selection  
- row limits  
- safe overwrite to staging  

Examples:

```bash
# 1-day smoke test
spark-submit iceberg_backfill_2024.py --sample --month 5 --day 16 --limit-rows 1000

# One month
spark-submit iceberg_backfill_2024.py --sample --month 8

# Full 2024 backfill
spark-submit iceberg_backfill_2024.py
```

---

# 5. Validation Plan

## 5.1 Row Count Validation

```sql
-- processed
SELECT COUNT(*)
FROM glue_catalog.trip_data
WHERE year = 2024 AND cab_type IN ('yellow','green');

-- staging
SELECT COUNT(*)
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024;
```

## 5.2 Min/Max Timestamp

```sql
SELECT MIN(pickup_datetime), MAX(pickup_datetime)
FROM nyc_taxi_wh.trip_data_v2_stage;
```

## 5.3 Null Checks

```sql
SELECT
  COUNT(*) FILTER (WHERE total_amount IS NULL) AS total_amount_nulls,
  COUNT(*) FILTER (WHERE pickup_datetime IS NULL) AS pickup_nulls
FROM nyc_taxi_wh.trip_data_v2_stage;
```

## 5.4 Spot Checks

```sql
SELECT *
FROM nyc_taxi_wh.trip_data_v2_stage
ORDER BY pickup_datetime
LIMIT 50;
```

---

# 6. Promotion to Production

## Case A: Prod missing 2024 → append

```sql
INSERT INTO nyc_taxi_wh.trip_data
SELECT *
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024;
```

## Case B: Prod has 2024 → replace 2024

```sql
CREATE OR REPLACE TEMP VIEW trip_data_merge AS
SELECT * FROM nyc_taxi_wh.trip_data
WHERE year(pickup_datetime) <> 2024
UNION ALL
SELECT * FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024;

INSERT OVERWRITE TABLE nyc_taxi_wh.trip_data
SELECT * FROM trip_data_merge;
```

---

# 7. Rollback Procedures

### If staging fails

```
DROP TABLE nyc_taxi_wh.trip_data_v2_stage;
```

### If production promotion fails

Use Iceberg snapshots:

```sql
CALL nyc_taxi_wh.trip_data.rollback_to_snapshot(<snapshot_id>);
```

---

# 8. Acceptance Criteria

- Schema aligned  
- Row counts match processed  
- Min/max timestamps correct  
- Null checks acceptable  
- Sample mode validated  
- Full 2024 validated  
- Promotion executed & validated  
- Snapshot before/after promotion  

---

# 9. Checklist Before Running Full Backfill

- [ ] Run 1-day smoke test  
- [ ] Run 1-month sample  
- [ ] Validate V2 stage  
- [ ] Run full 2024  
- [ ] Validate again  
- [ ] Promote to prod  
- [ ] Snapshot prod before/after  

---

**End of document.**
