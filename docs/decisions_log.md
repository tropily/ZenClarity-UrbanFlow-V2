# ZenClarity UrbanFlow V2 — Design Decisions Log
Updated: March 2026
Nguyen Le | ZenClarity Consulting
github.com/tropily/ZenClarity-UrbanFlow-V2

> This log documents key architectural and engineering decisions
> made during the design and build of ZenClarity UrbanFlow V2.
> Each entry captures the context, options considered, decision
> made, and business impact. Intended for onboarding, code review,
> and portfolio reference.

---

## Decision 1: Storage Format — Apache Iceberg vs Parquet/Hive

**Date:** Q4 2025 — V2 design phase

**Context:** V1 pipeline was already in production using partitioned
Parquet on S3 at s3://[your-s3-bucket]/processed/trip_data/ with
Hive-style partitioning registered in AWS Glue Data Catalog as
teo_nyc_taxi_db.trip_data. The Glue catalog managed all metadata
for the V1 Parquet tables — partition discovery, schema, and
Athena query access. Decision needed on whether to continue
with V1 Parquet or migrate to a modern table format for V2
while preserving the existing Glue catalog integration.

**Options considered:**
- Parquet on S3 with Hive partitioning — V1 baseline, already
  working, Glue catalog already managing metadata
- Apache Iceberg on S3 with Glue Data Catalog — modern table
  format, same catalog, new warehouse path

**Decision:** Apache Iceberg. V1 Parquet was functional but
lacked key capabilities needed for V2 scale and future roadmap:
- ACID transactions — safe concurrent reads and writes
- Schema evolution — add columns without rewriting partitions
- Partition evolution — change partition strategy without migration
- Time travel — query historical snapshots for audit and debugging
- Row-level deletes — required for GDPR and data correction
- Cross-engine reads — same S3 files readable by Glue, EMR,
  Snowflake external volume, and Athena without copying data

**Paths:**
- V2 Iceberg: s3://[your-s3-bucket]/warehouse/nyc_taxi_wh/
- V1 Parquet: s3://[your-s3-bucket]/processed/trip_data/
  — V1 Glue job still active on /processed/ during cutover
  — re-point to Iceberg target in progress (WBS 6.1)

**Impact:** trip_data_v2_stage partitioned on day(pickup_datetime).
42M+ records migrated from Parquet to Iceberg via backfill
framework. Glue catalog manages both V1 Parquet and V2 Iceberg
metadata during transition period. Snowflake reads same S3
Parquet files written by Glue and EMR via external volume —
no data copy, no duplication. Foundation for cross-engine
analytics confirmed.

---

## Decision 1b: Migration Strategy — Staged Table vs Direct

**Date:** Q4 2025
**Context:** 42M+ record migration required safe cutover strategy
with validation window before production impact.
**Decision:** Staged migration via trip_data_v2_stage. Backfill
and validate in parallel with production. Cut over when confirmed
clean. Pattern drawn from United Airlines EDW cutover approval
process — schema change migrations always validated in staging
before production promotion.
**Impact:** Zero production impact during backfill. Full rollback
available. dbt layer validated against staging before cutover.
V1 Parquet untouched until cutover confirmed complete.


---

## Decision 2: Idempotency Store — DynamoDB

**Date:** Q4 2025
**Context:** Pipeline needed idempotency tracking to prevent
duplicate processing when re-running day, month, or year slices.
Required fast key lookups and batch reads at day-granularity.
**Decision:** DynamoDB. Key-value store with batch_get_item
supports up to 100 items per call — each item is one day-level
audit record identified by slice_id. Low latency, serverless,
no schema overhead. S3 marker files considered but lacked
atomic batch reads.
**Impact:** UrbanFlow_Migration_Audit table tracks every day
slice with LANDED status. Batch read pattern prevents sequential
get_item loops. Re-runs skip already-LANDED slices automatically.
Benchmark results reflect engine performance only — DynamoDB
idempotency overhead is negligible at ~200ms for a full year
slice of 366 keys across 4 batch_get_item calls.

---

## Decision 3: Audit Key Granularity — day-level keys

**Date:** Q4 2025
**Context:** Month and year slices expand to multiple days.
Running a month slice after individual day runs risked
double-processing already-LANDED days without day-level
granularity.
**Decision:** Day-granularity keys — cab_type#yyyy_mm_dd.
expand_slice_to_day_keys() resolves any input to day-level list.
Idempotency check skips LANDED days regardless of slice size.
**Impact:** Overlap scenarios fully prevented. Month run after
day run skips correctly — validated in production. Partial slice
support via XCom — pending keys only passed to engine.

---

## Decision 4: Audit Write Ownership — DAG owns all writes

**Date:** Q4 2025
**Context:** Initial design had Glue and EMR scripts writing
LANDED audit records after job completion. This created dual
responsibility — scripts handled both data processing and audit.
**Decision:** DAG owns all audit writes. Glue and EMR scripts
are audit-free — single responsibility. write_audit_landed()
task fires after engine success with retries=3 + retry_delay=30s.
**Impact:** Clean separation of concerns. Scripts testable
independently. Audit failures handled by DAG retry logic without
re-running the engine. Easier to debug and maintain.

**Known gap:** If engine succeeds but write_audit_landed fails
after all retries — Iceberg partition exists but DynamoDB has
no LANDED record. Next run re-processes those days via
overwritePartitions — correct result, slight inefficiency.
Reconciliation DAG planned in V3 to detect and heal this
drift without engine re-run.

---

## Decision 5: Volumetric Routing Threshold — 0.05 GB

**Date:** Q4 2025
**Context:** Cost-aware engine selection required a configurable
threshold to route slices between Glue and EMR based on data
volume. EMR has cluster spin-up overhead — not justified for
small slices.
**Decision:** 0.05 GB demo threshold. Intentionally conservative
to showcase routing logic across all slice granularities.
Production threshold must be calibrated against actual EMR
cluster cost vs Glue DPU pricing at target data volumes.
**Impact:** Day slices → Glue Net Play. Month/year slices →
EMR Heavy Serve. Both engines confirmed working in production.
4× runtime improvement at 42M record scale.

---

## Decision 6: Snapshot Strategy — check vs timestamp

**Date:** March 2026
**Context:** Needed to track history of vendor reference data
loaded via dbt seed. No upstream system provides vendor data —
seed is the source of truth.
**Options considered:**
- Timestamp strategy — requires updated_at column from source
- Check strategy — compares column values directly between runs
**Decision:** Check strategy. The vendor seed has no updated_at
column — no upstream system stamps it. Check strategy detects
changes by comparing vendor_name and status values between
snapshot runs. Right tool when source has no reliable timestamp.
**check_cols:** vendor_name, status — the two mutable columns
that can change over time.
**unique_key:** vendor_id — immutable, never changes, safe anchor.
**Impact:** snap_vendor tracks full vendor history in SNAPSHOTS
schema. dim_vendor filters dbt_valid_to IS NULL for current state.
SCD Type 2 simulation proven — CMT status active → suspended,
old row closed, new row inserted confirmed March 2026.

---

## Decision 7: Vendor Data Source — seed vs pipeline

**Date:** March 2026
**Context:** Vendor reference data has only 2 known values.
No vendor reference file exists on the NYC TLC data portal.
Decision needed on source of truth for all engines.
**Decision:** dbt seed for Snowflake dbt layer demonstration.
Production pattern requires vendor.csv in S3 registered in
Glue catalog — single source of truth readable by Glue, EMR,
Athena, and Snowflake via external volume. Seed demonstrates
the dbt pattern but does not replace S3 as authoritative source
for cross-engine consumption.
**Production path:** vendor.csv → S3 → Glue catalog →
all engines. Snowflake reads via RAW_ICEBERG external volume.
**Impact:** Seed confirms dbt SCD Type 2 snapshot pattern.
Production implementation requires S3 upload and Glue catalog
registration to make vendor lookup available to Glue and EMR jobs.

---

## Decision 8: dim_vendor — Current State Only vs Full History

**Date:** March 2026
**Context:** snap_vendor contains full vendor history including
closed rows. Decision needed on what dim_vendor exposes to
downstream consumers.
**Decision:** Current state only. dim_vendor filters
dbt_valid_to IS NULL. Full history stays in snap_vendor for
point-in-time joins. BI team gets a clean two-row dimension.
**Impact:** dim_vendor always shows 2 rows — one per vendor,
current status only. Historical analysis goes direct to
snap_vendor with valid date range filter.

---

## Decision 9: fare_amount Quality Filter — >= 0 vs > 0

**Date:** March 2026
**Context:** dbt_expectations test exposed 3,712 fare_amount
anomalies. Investigation revealed 3,710 zero fare records with
legitimate trip duration and positive total_amount, and 2 records
with genuinely negative fare.
**Decision:** fare_amount >= 0. Zero fare is allowed — legitimate
trips exist with zero metered fare but positive total_amount.
Negative fare is bad data. total_amount > 0 remains primary
quality gate.
**Impact:** 2 negative fare records routed to quarantine.
3,710 zero fare trips preserved in mart. 17/17 tests passing
after full refresh.

---

## Decision 10: dbt Packages — dbt_utils + dbt_expectations

**Date:** March 2026
**Context:** Manual surrogate key using md5 concat was working
but non-standard. Basic dbt tests could not enforce threshold
rules like fare_amount > 0 or passenger_count between 1 and 6.
**Decision:** dbt_utils for surrogate key generation —
community standard, cross-database compatible. dbt_expectations
for threshold-based DQ tests — mirrors quality filters already
in int_trip_data_core.sql.
**Impact:** Surrogate key generation standardized. 4 threshold
tests added to int_trip_data_core.yml. 17/17 tests passing.
DQ layer mirrors SQL quality filters — tests confirm mart is clean.

---

## Decision 11: Exposures — marts.yml vs separate file

**Date:** March 2026
**Context:** Needed to document downstream consumers of mart
models for lineage visibility. Decision on file structure.
**Decision:** Add exposures block directly to marts.yml.
Project has 2 exposures — Streamlit dashboard and QuickSight
report. Separate file adds overhead without benefit at this scale.
**Impact:** Lineage graph extends from RAW_ICEBERG source to
Streamlit and QuickSight consumers. Any change to fact_trip
shows downstream impact in dbt docs immediately.

---

## Decision 12: Snowflake — Read-Only Consumer vs Read-Write

**Date:** Q4 2025
**Context:** Snowflake needed access to Iceberg tables on S3.
Decision on whether Snowflake should be a read-only consumer
or have write access to the Iceberg warehouse path.
**Decision:** Read-only. ALLOW_WRITES=FALSE enforced at external
volume level. Snowflake reads the same S3 Parquet files written
by Glue and EMR — no data copy, no duplication. Write access
would create competing writers and risk Iceberg metadata corruption.
**Impact:** EV_NYC_ICEBERG external volume enforces read-only.
DBT_DEV_ROLE has SELECT on RAW_ICEBERG only. Zero risk of
Snowflake overwriting Iceberg files written by Glue or EMR.

---

## Decision 13: dbt Incremental Strategy — unique_key MERGE

**Date:** March 2026
**Context:** int_trip_data_core processes 35.6M+ records.
Full refresh on every run would cost ~60 seconds of Snowflake
compute per run. Delta runs needed to process only new records.
**Decision:** Incremental materialization with unique_key=trip_id.
Delta runs filter on ingestion_ts > max(ingestion_ts) in target.
Full refresh reserved for quality filter changes or schema updates
that affect historical records.
**Impact:** Delta runs complete in ~3 seconds vs 60 seconds full
refresh. Full refresh used sparingly — only when logic changes
affect existing records.

---

## Decision 14: Benchmark — Staged Optimization vs Single Run

**Date:** Q4 2025
**Context:** EMR performance needed to be measured rigorously
to justify routing decision and document improvement story.
Single benchmark run would not isolate which optimization
drove the improvement.
**Decision:** Three-stage benchmark — engine swap baseline,
executor tuning, DAG optimization. Each stage isolated one
variable. Spark UI profiling used to identify bottleneck
between stages.
**Impact:** 4× improvement over Glue baseline documented with
evidence. Optimization story traceable — DAG simplification
drove the gain, not cluster resize. Benchmark results published
in README with decision threshold rationale.

---

## Decision 15: TTL — Removed from DynamoDB Audit Records

**Date:** Q4 2025
**Context:** Initial DynamoDB design included TTL of 90 days
on audit records to manage storage costs. Concern raised that
expiring records would lose lineage and break idempotency checks
for re-runs after 90 days.
**Decision:** TTL removed. Audit records are permanent.
DynamoDB storage cost for day-granularity keys across all cab
types and full year is negligible. Full lineage preserved
indefinitely.
**Impact:** UrbanFlow_Migration_Audit records never expire.
Re-runs at any point in the future correctly skip LANDED slices.
Full audit trail available for debugging and reconciliation.

---

## Template for future decisions

---

## Decision N: [Title]

**Date:**
**Context:**
**Options considered:**
-
-
**Decision:**
**Impact:** 