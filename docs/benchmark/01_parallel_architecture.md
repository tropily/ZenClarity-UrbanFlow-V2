# 01 — Parallel Warehouse Architecture
## ZenClarity UrbanFlow V2 — Snowflake vs Redshift Benchmark Series

## Overview
Both Snowflake and Redshift point at the same Apache Iceberg files on S3
via the AWS Glue Data Catalog. Neither warehouse owns the data. Both engines 
read the same physical parquet files via AWS Glue Data Catalog. Transformations 
are identical dbt models run against two profile targets from a single codebase. 

## Architecture

S3 Iceberg (Glue Catalog — nyc_taxi_wh)
        │
        ├── Snowflake External Volume (INTG_GLUE_NYC_ICEBERG)
        │         → RAW_ICEBERG.TRIP_DATA
        │         → dbt target: snowflake_iceberg
        │         → STG_NYC_TAXI / INT_NYC_TAXI / MART_NYC_TAXI
        │
        └── Redshift Spectrum External Schema (spectrum_nyc_taxi)
                  → spectrum_nyc_taxi.trip_data
                  → dbt target: redshift_dev
                  → dev.stg_nyc_taxi / dev.int_nyc_taxi / dev.mart_nyc_taxi

---

## Source Data

| Attribute | Value |
|-----------|-------|
| Dataset | NYC Taxi trip records 2024 |
| Record count | 41,169,300 |
| Format | Apache Iceberg V2 |
| Storage | Amazon S3 — teo-nyc-taxi bucket |
| Catalog | AWS Glue — nyc_taxi_wh database |
| Tables | trip_data + taxi_zone_lookup |
| Partition | day(pickup_datetime) |
| Cab types | yellow, green, fhv, high_volume_fhv |

---

## Snowflake Connection

Snowflake reads Iceberg via an External Volume + Catalog Integration —
three infrastructure components that grant Snowflake read-only access
to S3 Iceberg files via the Glue Catalog. Zero data movement.
ALLOW_WRITES=FALSE enforced at the volume level.

**Components:**
- Storage Integration — S3 read access via IAM trust policy
- Catalog Integration — Glue Catalog access scoped to nyc_taxi_wh
- External Volume — points at s3://teo-nyc-taxi/warehouse/nyc_taxi_wh/

---

## Redshift Connection

Redshift reads the same Iceberg files via Spectrum + an external schema
pointing at the Glue Catalog — a single SQL command that creates a
read-only bridge from Redshift into the S3 Iceberg tables.

**IAM Role — teo_redshift_service_role**
Attached to the Redshift namespace with scoped S3 read and Glue Catalog
access — least-privilege, no write permissions.

**Workgroup — teo-nyc-workgroup**
- Base RPU: 8
- Namespace: teo-nyc-namespace
- Auto-pause: enabled (dev environment)

---

## Compute Configuration

| Engine | Config | Approx Equivalent | Hourly Cost |
|--------|--------|-------------------|-------------|
| Snowflake | X-Small warehouse (1 credit/hr) | ~4 vCores | ~$2.00/hr |
| Redshift | Serverless 8 RPU | ~4 vCores | $2.88/hr |

Both engines benchmarked at minimum viable compute tier.
This is a cost-performance comparison — not a maximum throughput test.

---

## dbt Configuration

| Attribute | Snowflake | Redshift |
|-----------|-----------|----------|
| dbt codebase | Single project | Single project |
| Profile target | snowflake_iceberg | redshift_dev |
| Source database | NYC_TAXI_DEV | nyc_taxi_db |
| Source schema | RAW_ICEBERG | spectrum_nyc_taxi |
| Staging schema | STG_NYC_TAXI | stg_nyc_taxi |
| Intermediate schema | INT_NYC_TAXI | int_nyc_taxi |
| Mart schema | MART_NYC_TAXI | mart_nyc_taxi |
| Snapshot schema | SNAPSHOTS | snapshots |

---

## Key Design Decision

**Why not copy data into each warehouse?**

Copying 41M Iceberg records into both Snowflake and Redshift would:
- Introduce data drift — two copies can diverge over time
- Add storage cost on both engines
- Invalidate the benchmark — different physical data, different results

Reading from the same S3 Iceberg files guarantees the benchmark
measures engine performance, not data differences.

**Why the benchmark is apples-to-apples at query time:**

All five benchmark queries run against the mart layer — fact_trip,
dim_taxi_zone, and dim_date — which are physical materialized tables
on both engines. The staging materialization difference affects build
time only, not query performance. See 02_materialization_decisions.md
for the full explanation.

---

## Connection Comparison

| | Snowflake | Redshift |
|---|---|---|
| Connection type | External Volume | Spectrum External Schema |
| Metadata source | Glue via Catalog Integration | Glue Catalog directly |
| Data location | S3 (read-only) | S3 (read-only) |
| Write access | ALLOW_WRITES=FALSE | IAM read-only policy |
| Zero copy | Yes | Yes |
| Setup complexity | Higher (3 components) | Lower (1 SQL command) |
| Auto-refresh | AUTO_REFRESH=TRUE | Automatic via Glue |
| Staging materialization | Zero-cost view | Incremental table |
| Benchmark query layer | Mart (physical table) | Mart (physical table) |