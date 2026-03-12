# 🌆 ZenClarity-UrbanFlow — NYC Taxi Data Engineering Platform
> **A modern data engineering platform** combining streaming + batch pipelines, dbt-powered transformations,
> and multi-engine analytics across **Redshift Serverless**, **Snowflake**, and **EMR Spark.**
> Designed for **portability**, **scalability**, **cost-performance benchmarking**, and **real-time insights** delivered via Streamlit.

---

## 🚀 V2 — Production Delivery Status

| Component | Status | Notes |
|---|---|---|
| Iceberg staging table — `trip_data_v2_stage` | ✅ Confirmed | Partitioned on `day(pickup_datetime)` |
| Glue backfill job | ✅ Confirmed | Serverless · schema-aligned · audit-free |
| EMR Spark backfill job | ✅ Confirmed | 4× faster than Glue at scale |
| Glue vs EMR benchmark | ✅ Confirmed | 42M records · 6 min vs 1.5 min · threshold configurable |
| Airflow DAG — volumetric router | ✅ Confirmed | Cost-aware engine selection · both engines live |
| DynamoDB idempotency audit | ✅ Confirmed | Day-granularity · permanent records · batch reads |
| Monthly delta ingestion → Iceberg | 🔧 In Progress | V1 Glue job still active on `/processed/` · re-point underway |
| Airflow DAG — full pipeline cutover | 🔧 In Progress | Replaces Step Functions · adds dbt downstream |
| dbt model update — schema alignment | 🔧 In Progress | New V2 columns added · staging models need re-point |
| Snowflake Iceberg integration | ⬡ Next | External volume + storage integration |
| CI/CD — GitHub Actions | ○ Planned | dbt test on PR · deploy on merge |

---

## 🏗️ V2 Core Design — Iceberg Backfill & Migration Framework

### What Was Built
A production-grade, cost-aware backfill framework migrating **42M+ NYC Taxi records** into **Apache Iceberg**
with full idempotency, multi-engine routing, and audit traceability.

### How It Works

```
Airflow DAG — engine_volumetric_router
       │
       ├─ Gate 1: DynamoDB idempotency check
       │          → expand any slice (year/month/day) to day-level keys
       │          → batch read 100 keys/call — skip all LANDED slices
       │
       ├─ Gate 2: S3 volumetric scan
       │          → size > 0.05 GB  →  EMR Heavy Serve  
       │          → size ≤ 0.05 GB  →  Glue Net Play    
       │          → empty slice     →  safe exit
       │
       ├─ Engine fires → reads S3 parquet → aligns schema → writes Iceberg
       │
       └─ write_audit_landed → batch writes LANDED records to DynamoDB
                               retries=3 · retry_delay=30s
```

### Key Design Decisions
- **DAG owns all audit writes** — Glue and EMR scripts are audit-free, single-responsibility
- **Day-granularity idempotency** — month/year slices expand to day keys, preventing overlap double-writes
- **Permanent audit records** — TTL removed, full lineage preserved
- **batch_id format:** `engine#cab_type#yyyy_mm_ref#uuid8` — engine + slice traceable in every record

### Benchmark Results

Benchmark run on full 2024 NYC Taxi dataset — **42M records** across all cab types.

| Engine | Dataset | Runtime |
|---|---|---|
| EMR Spark | Full year 2024 — 42M records | ~1.5 min |
| AWS Glue | Full year 2024 — 42M records | ~6 min |

> **EMR is 4× faster at scale.** At 42M records, EMR Spark is the clear performance winner.

### Volumetric Routing Threshold

The DAG uses a configurable S3 size scan to select the engine at runtime.
The current demo threshold is set at **0.05 GB** — intentionally conservative
to showcase the routing logic across all slice granularities (day / month / year).

> ⚠️ **Note:** In production, this threshold should be calibrated against
> actual EMR cluster cost vs Glue DPU pricing at target data volumes.
> Spark's cluster spin-up overhead means the true cost crossover point
> occurs at significantly higher volume than 0.05 GB.
> This framework demonstrates the **routing pattern and scalability** —
> the threshold is a tunable parameter, not a fixed boundary.

---

## 🗺️ Architecture
![Architecture Diagram](docs/arch_diagrams/ZenClarity-UrbanFlow_architecture.jpg)

---

## 🌐 Portability — One dbt Codebase → Three Engines
> One dbt codebase runs on **Snowflake**, **Redshift**, and **EMR Spark** —
> true engine flexibility with no rewrites.

![Portability Overview](docs/arch_diagrams/portability_overview.jpg)

**Why it matters**
- Avoids vendor lock-in and simplifies migrations
- Enables apples-to-apples benchmarking across engines
- Keeps analytics consistent and DRY with shared models and macros

---

## 📊 Project Highlights

### Data Ingestion
- **Streaming:** Python simulator + Kinesis Firehose for near real-time ingestion
- **Batch:**
  - AWS Glue — serverless ETL for small-to-medium payloads
  - EMR Spark — distributed batch processing for large-scale backfill

### Data Lake & Storage
- Central **Amazon S3** data lake with **Apache Iceberg** table format (V2)
- **DynamoDB** — idempotency audit table (`UrbanFlow_Migration_Audit`)
  - Day-granularity slice tracking · permanent records · batch read pattern

### Data Transformation
- ETL: AWS Glue + EMR Spark
- ELT: dbt multi-layer (staging → intermediate → marts)
- V2: dbt incremental models on Iceberg source (in progress)

### Data Warehousing
- **Redshift Serverless** — streaming and batch analytics
- **Snowflake** — bulk loading, benchmarking, Iceberg external tables (next)
- **EMR Spark SQL** — distributed queries and performance testing

---

## ⚙️ Orchestration

### V2 — Airflow Volumetric Router (Current)
**DAG:** `engine_volumetric_router`
- Cost-aware engine selection at runtime based on S3 slice size
- DynamoDB idempotency gate — prevents duplicate processing
- Partial slice support — pending keys passed via XCom
- Both engines confirmed working in production

### V1 — Step Functions + Airflow (Baseline)
- **AWS Step Functions** — production Glue-based pipeline to Redshift
- **Apache Airflow (Docker)** — EMR Spark batch runs for custom workloads

---

## 📂 Repo Structure

```text
ZenClarity-UrbanFlow/
├─ iceberg_backfill_migration_framework/   ← V2 NEW
│  ├─ scripts/
│  │  ├─ engine_volumetric_router.py       ← Airflow DAG
│  │  ├─ glue_iceberg_backfill_migration.py
│  │  ├─ emr_iceberg_backfill_migration.py
│  │  └─ iceberg_migration_utils.py
│  └─ README.md                            ← Framework deep-dive
├─ analytics/
├─ config/
├─ dbt/
├─ docs/
│  ├─ arch_diagrams/
│  ├─ benchmarks/
│  ├─ metrics/
│  └─ runbooks/
├─ infrastructure/
│  ├─ emr/
│  ├─ glue/
│  ├─ redshift/
│  └─ snowflake/
├─ scripts/
│  ├─ airflow/
│  ├─ batch/
│  ├─ emr_jobs/
│  ├─ streaming/
│  └─ helpers/
├─ tools/
│  └─ airflow-docker/
└─ README.md
```

---

## 📈 dbt Modeling

> Multi-layered dbt pattern — staging → intermediate → marts

- **Staging:** cleans raw data, enforces schema
- **Intermediate:** joins and transformations
- **Marts:** business-defined facts and dimensions

**V2 Update:** Incremental models targeting `trip_data_v2_stage` (Iceberg) — in progress.

📑 [View dbt Project Documentation (S3 Hosted)](http://nle-dbt-docs.s3-website-us-east-1.amazonaws.com/#!/overview)

---

## 📊 Dashboard KPIs (Streamlit)
- Trips count · Total fare revenue · Average trip delay
- Passengers carried · Trips per minute
- Real-time vs baseline comparison · Cumulative trip chart

![Dashboard Screenshot](docs/metrics/streamlit_live_streaming_dashboard.jpg)

---

## 🌐 Technologies Used

**AWS:** S3 · Kinesis Firehose · Glue · Lambda · Step Functions · EventBridge
· DynamoDB · Athena · Redshift Serverless · EMR (Spark, Hive) · Apache Iceberg

**Other:** dbt · Snowflake · Airflow · Python · PySpark · Streamlit

---

## 📚 Roadmap

**V2 Phase 1 — Confirmed ✅**
- Iceberg staging table + backfill framework (Glue + EMR)
- Cost-aware Airflow DAG with DynamoDB idempotency audit
- Both engines benchmarked and confirmed working

**V2 Phase 2 — In Progress 🔧**
- Monthly delta ingestion Glue job re-pointed to Iceberg (replaces V1 `/processed/` path)
- Airflow DAG replacing Step Functions — adds dbt downstream of engine success
- dbt incremental models updated for V2 schema (new metadata columns)
- Dashboard update — Streamlit + QuickSight on V2 mart layer

**V2 Phase 3 — Next ⬡**
- Snowflake Iceberg external tables + storage integration
- Benchmark: Snowflake Iceberg vs original external table
- Benchmark: Redshift Spectrum vs Snowflake external table
- CI/CD — GitHub Actions (dbt test on PR · deploy on merge)

**V3 — Planned ○**
- Reconciliation DAG — DynamoDB LANDED vs Iceberg partition drift detection
- Data quality layer — dbt-expectations + referential integrity tests
- Predictive analytics — surge demand zones

---

## 💡 Inspiration
> *"ZenClarity-UrbanFlow embodies the idea that modern data engineering should empower everyone —
> from engineers to analysts — with scalable pipelines, portable models, and AI-driven access
> to insights with Clarity."*

---

## 🔗 Connect
- LinkedIn: [le-nguyen-v](https://www.linkedin.com/in/le-nguyen-v/)
- GitHub: [tropily](https://github.com/tropily/ZenClarity-UrbanFlow)
