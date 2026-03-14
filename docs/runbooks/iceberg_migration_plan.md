# Iceberg Migration Runbook — 2024 Backfill (ZenClarity-UrbanFlow V2)

**Owner:** Nguyen Le
**Last Updated:** March 2026
**Scope:** Migrate 2024 NYC Taxi trip data (all 4 cab types) from processed zone
→ Iceberg warehouse staging table (`trip_data_v2_stage`), validate,
then promote to production (`trip_data`).
**Status:** ✅ Phase 1 Complete — 42M records migrated and validated

---

## 1. Architecture Overview

```
S3 Processed Zone (Parquet, partitioned by cab_type/year/month/day)
    ↓
Airflow DAG — engine_volumetric_router
    ├─ Gate 1: DynamoDB Idempotency Check
    └─ Gate 2: S3 Volumetric Scan → EMR or Glue
         ↓                ↓
    EMR Spark 7.7.0   AWS Glue 4.0
    repartition(60)   Serverless
         ↓                ↓
    Schema Alignment — align_to_iceberg_schema()
         ↓
    Iceberg Staging Table — trip_data_v2_stage
         ↓
    DynamoDB Audit Write — status = LANDED
         ↓
    Validation Suite
         ↓
    Promotion → Production Iceberg Table — trip_data
         ↓
    Snowflake External Iceberg Table
         ↓
    dbt Medallion Stack (staging → intermediate → marts)
```

---

## 2. Source → Destination Definition

### 2.1 Source — S3 Processed Zone

```
s3://****/processed/trip_data/
  cab_type=yellow/year=2024/month=1/day=1/part-xxxx.snappy.parquet
  cab_type=green/year=2024/month=1/day=1/part-xxxx.snappy.parquet
  cab_type=fhv/year=2024/month=1/day=1/part-xxxx.snappy.parquet
  cab_type=high_volume_fhv/year=2024/month=1/day=1/part-xxxx.snappy.parquet
```

All 4 cab types confirmed ✅

### 2.2 Destination Tables

**Staging (Phase 1 write target):**
```
nyc_taxi_wh.trip_data_v2_stage
LOCATION: s3://****/warehouse/nyc_taxi_wh/trip_data_v2_stage
PARTITIONED BY: day(pickup_datetime)
```

**Production (Phase 1 confirmed):**
```
nyc_taxi_wh.trip_data
LOCATION: s3://****/warehouse/nyc_taxi_wh/trip_data
PARTITIONED BY: day(pickup_datetime)
```

### 2.3 Final Schema — 26 Columns

| Column | Type | Notes |
|--------|------|-------|
| vendorid | int | |
| cab_type | string | yellow/green/fhv/high_volume_fhv |
| pickup_datetime | timestamp | partition key |
| dropoff_datetime | timestamp | |
| store_and_fwd_flag | string | |
| ratecodeid | bigint | |
| pulocationid | int | |
| dolocationid | int | |
| passenger_count | bigint | |
| trip_distance | double | |
| fare_amount | double | |
| extra | double | |
| mta_tax | double | |
| tip_amount | double | |
| tolls_amount | double | |
| ehail_fee | double | NULL if missing |
| improvement_surcharge | double | |
| total_amount | double | |
| payment_type | bigint | |
| trip_type | bigint | NULL if missing |
| congestion_surcharge | double | NULL if missing |
| airport_fee | double | NULL if missing |
| ingestion_ts | timestamp | pipeline arrival time — watermark |
| batch_id | string | engine#cab#yyyy_mm_dd#uuid8 |
| source_file | string | S3 path for lineage |
| migration_batch_id | string | batch reference |
| engine_type | string | EMR_7.7.0_MULTI_RES or GLUE_4.0_NETPLAY |

---

## 3. Orchestration — Airflow DAG

**DAG:** `engine_volumetric_router`
**File:** `iceberg_backfill_migration_framework/scripts/engine_volumetric_router.py`

### 3.1 Idempotency Design

```
Key format : {cab_type}#{yyyy}_{mm}_{dd}
Example    : yellow#2024_01_15

DynamoDB table: UrbanFlow_Migration_Audit
  slice_id   → partition key
  status     → LANDED
  batch_id   → engine#cab#yyyy_mm_dd#uuid8
  engine     → emr or glue
  landed_at  → ISO timestamp
  TTL        → removed — permanent records
```

### 3.2 Engine Routing

```
S3 volumetric scan → size_gb

size_gb > threshold → EMR Heavy Serve  (emr#...)
size_gb > 0         → Glue Net Play    (glue#...)
size_gb = 0         → Safe Exit

⚠️ Threshold Note:
The routing threshold is configurable and environment-specific.
AWS Glue 4.0 is production-grade and capable at scale.
The crossover point depends on:
  → EMR cluster spin-up cost vs Glue DPU pricing
  → Workload shape — shuffle-heavy favors EMR
  → Target data volumes
Calibrate against your own cost model before production use.
```

### 3.3 Engine Scripts

| Engine | Script | Key Feature |
|--------|--------|-------------|
| EMR Spark 7.7.0 | `emr_iceberg_backfill_migration.py` | repartition(60) before write |
| AWS Glue 4.0 | `glue_iceberg_backfill_migration.py` | Serverless · zero cluster overhead |

---

## 4. Schema Alignment

Both engines use `align_to_iceberg_schema()` — single SELECT with conditional columns:

```python
# Conditional columns — NULL if missing in source
ehail_fee            → F.col("ehail_fee") if "ehail_fee" in cols else F.lit(None)
trip_type            → F.col("trip_type") if "trip_type" in cols else F.lit(None)
congestion_surcharge → conditional
airport_fee          → conditional

# Metadata — always populated
ingestion_ts         → F.current_timestamp()
batch_id             → F.lit(BATCH_ID)
source_file          → F.lit(S3_PATH)
migration_batch_id   → F.lit(batch_id)
engine_type          → F.lit("EMR_7.7.0_MULTI_RES") or "GLUE_4.0_NETPLAY"
```

---

## 5. Benchmark Results

Full 2024 dataset — 42M records across all cab types.

| Engine | Version | Runtime | vs Glue Baseline |
|--------|---------|---------|-----------------|
| AWS Glue | Baseline | ~6 min | — |
| EMR Spark | Baseline | ~3m 16s | −46% |
| EMR Spark | Optimized Lean | ~1m 34s | −74% · 4× faster |

The biggest improvement came from DAG optimization — not hardware scaling.
Eliminating redundant scans, controlling write parallelism via `repartition(60)`,
and simplifying alignment logic cut runtime without cluster resize.

---

## 6. Iceberg Schema Evolution — Executed

After Phase 1 backfill, 4 metadata columns were added to production:

```sql
-- Step 1: Add columns (new metadata.json only — no data moved)
ALTER TABLE nyc_taxi_wh.trip_data
ADD COLUMNS (
    batch_id           string COMMENT 'Airflow DAG Run ID or Batch UUID',
    source_file        string COMMENT 'S3 Path for Lineage',
    migration_batch_id string,
    engine_type        string
);

-- Step 2: Delete existing 2024 data
DELETE FROM nyc_taxi_wh.trip_data
WHERE year(pickup_datetime) = 2024;

-- Step 3: Re-insert from staging with all 26 columns
INSERT INTO nyc_taxi_wh.trip_data
SELECT * FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024;

-- Step 4: Cleanup orphaned files
VACUUM nyc_taxi_wh.trip_data;
```

Result: 42M records · all 4 cab types · 26 columns · validated ✅

---

## 7. Validation Suite

### 7.1 Row Count

```sql
SELECT COUNT(*)
FROM nyc_taxi_wh.trip_data
WHERE year(pickup_datetime) = 2024;
-- Expected: ~42M records
```

### 7.2 Cab Type Distribution

```sql
SELECT cab_type, COUNT(*) as record_count
FROM nyc_taxi_wh.trip_data
WHERE year(pickup_datetime) = 2024
GROUP BY cab_type
ORDER BY record_count DESC;
-- Expected: all 4 cab types present
```

### 7.3 Metadata Column Validation

```sql
SELECT
  COUNT(*) FILTER (WHERE batch_id IS NULL)           AS batch_id_nulls,
  COUNT(*) FILTER (WHERE source_file IS NULL)        AS source_file_nulls,
  COUNT(*) FILTER (WHERE migration_batch_id IS NULL) AS migration_batch_id_nulls,
  COUNT(*) FILTER (WHERE engine_type IS NULL)        AS engine_type_nulls,
  COUNT(*) FILTER (WHERE ingestion_ts IS NULL)       AS ingestion_ts_nulls
FROM nyc_taxi_wh.trip_data
WHERE year(pickup_datetime) = 2024;
-- Expected: 0 nulls
```

### 7.4 Min/Max Timestamps

```sql
SELECT
  MIN(pickup_datetime) AS earliest_trip,
  MAX(pickup_datetime) AS latest_trip
FROM nyc_taxi_wh.trip_data
WHERE year(pickup_datetime) = 2024;
-- Expected: 2024-01-01 → 2024-12-31
```

### 7.5 dbt Test Suite

```bash
dbt test --select stg_trip_data --target snowflake_iceberg
# Expected: 8/8 passing

dbt test --select int_trip_data_core --target snowflake_iceberg
# Expected: 10/10 passing

dbt test --select fact_trip --target snowflake_iceberg
# Expected: all passing · 35.6M records
```

### 7.6 Snowflake Integration

```sql
SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime)
FROM NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA
WHERE year(pickup_datetime) = 2024;

SELECT COUNT(*) FROM NYC_TAXI_DEV.MART_NYC_TAXI.fact_trip;
-- Expected: 35.6M records after quality filtering + dedup
```

---

## 8. Rollback Procedures

### Staging write fails

```sql
-- Safe — production untouched
DROP TABLE nyc_taxi_wh.trip_data_v2_stage;
-- Fix issue, re-run DAG from scratch
```

### Production promotion fails

```sql
-- List available snapshots
SELECT * FROM nyc_taxi_wh.trip_data.history;

-- Roll back to pre-promotion snapshot
CALL nyc_taxi_wh.system.rollback_to_snapshot(
  'nyc_taxi_wh.trip_data',
  <snapshot_id>
);
```

### DynamoDB audit out of sync

```python
# Manually reset day keys to PENDING for re-processing
# batch_get_item to identify affected keys
# put_item to reset status = PENDING
# Re-trigger DAG for affected slice
```

---

## 9. Acceptance Criteria

| Criteria | Status |
|----------|--------|
| All 4 cab types migrated | ✅ Confirmed |
| 42M records in production | ✅ Confirmed |
| 26-column schema aligned | ✅ Confirmed |
| Metadata columns populated | ✅ Confirmed |
| DynamoDB all keys LANDED | ✅ Confirmed |
| Iceberg time travel working | ✅ Confirmed |
| Snowflake external table reads | ✅ Confirmed |
| dbt staging 8/8 tests pass | ✅ Confirmed |
| dbt intermediate 10/10 tests pass | ✅ Confirmed |
| dbt mart fact_trip 35.6M records | ✅ Confirmed |

---

**End of document.**
