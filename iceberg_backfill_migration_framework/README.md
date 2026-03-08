## Iceberg Backfill Migration Framework (Glue + EMR Spark)

This project includes a **reusable backfill framework** for migrating large historical
datasets from partitioned Parquet on S3 into **Iceberg tables** managed by the AWS
Glue Data Catalog.

### What It Does

- Migrates **42M+ NYC Taxi trip records (year 2024)** from:
  - **Source:** `teo_nyc_taxi_db.trip_data` (Parquet on S3, Hive/Glue catalog)
  - **Target:** `nyc_taxi_wh.trip_data_v2_stage` (Iceberg table on S3, Glue catalog)
- Supports both **AWS Glue** and **EMR Spark** using the same logical pipeline:
  - Glue job: `glue_iceberg_backfill_migration_2024.py`
  - EMR job: `emr_iceberg_backfill_migration_2024.py`

### Backfill Contract

The framework is parameterized so the same code can handle:

- **Full year:** `2024`
- **Specific month:** `2024-06`
- **Single day:** `2024-06-15`
- Optional `ROW_LIMIT` for small-slice testing

Parameters:

- `BACKFILL_YEAR` (required)
- `BACKFILL_MONTH` (optional, or "None")
- `BACKFILL_DAY` (optional, or "None")
- `ROW_LIMIT` (optional, or "None")

### High-Level Flow

1. **Slice Selection**
   - Filters the source table by `year`, `month`, `day` (when provided).
   - Supports 1-day, 1-month, or full-year slices.

2. **Schema Alignment**
   - Maps the source Parquet schema to the Iceberg schema:
     - Ensures consistent types (e.g., `double`, `bigint`, `timestamp`).
     - Fills missing optional fields (`ehail_fee`, `airport_fee`, etc.) with `NULL`.
     - Adds `ingestion_ts` for lineage and troubleshooting.

3. **Write to Iceberg**
   - Writes into `nyc_taxi_wh.trip_data_v2_stage` via:
     - `DataFrameWriterV2.writeTo(...).overwritePartitions()`
   - Uses **repartitioning** to balance parallelism and reduce small files.

4. **Validation**
   - Spark + Athena validation comparing source vs target:
     - Row counts per `cab_type` and time slice
     - `SUM(total_amount)` by `cab_type`
     - `pickup_datetime` min/max ranges

### Performance Highlights

- **Glue baseline:** ~6 minutes to migrate the 2024 slice with `10 × G.1X` workers.
- **EMR Spark baseline:** ~3m16s on an m5.xlarge-based cluster.
- **EMR Spark (optimized/lean):** ~**1m34s**, a **4× improvement** vs Glue baseline via:
  - DAG simplification (removing per-cab unions)
  - Early filter pushdown (year/month/day)
  - Targeted repartitioning before the Iceberg write

This framework is designed to be **reusable** across tables and environments:
teams can plug in a different source table, adjust the schema alignment function,
and reuse the same backfill + validation pattern to onboard new datasets into Iceberg.
