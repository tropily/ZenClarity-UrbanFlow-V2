# Snowflake vs Redshift Spectrum — Benchmark Series
## ZenClarity UrbanFlow V2

This folder documents a benchmark comparing Snowflake and
Amazon Redshift Spectrum against the same Apache Iceberg dataset on S3.
Both engines run identical dbt models from a single codebase — no rewrites,
no separate pipelines.

---

## The Setup in One Sentence

> Two warehouse engines. One S3 Iceberg source. One dbt codebase. Five queries.

---

## Why This Benchmark Matters

Most warehouse comparisons are synthetic — different datasets, different schemas,
different query patterns. This benchmark eliminates those variables:

- **Same source data** — 41.1M NYC Taxi records read from S3 Iceberg via Glue Catalog
- **Same transformation logic** — single dbt project, two profile targets
- **Same compute tier** — Snowflake XS vs Redshift Serverless 8 RPU (~4 vCores each)
- **Same queries** — identical SQL against both mart layers

---

## Headline Results

| Metric | Snowflake XS | Redshift 8 RPU | Winner |
|--------|-------------|----------------|--------|
| dbt full build | 14.65s | 36.93s | Snowflake 2.5x |
| Q1 cold — full scan | 2.105s | 26.000s | Snowflake 12x |
| Q2 cold — date filter | 0.885s | 0.451s | Redshift 2x |
| Q3 warm — join query | 0.507s | 0.054s | Redshift 9x |
| Q1 warm — full scan | 0.461s | 0.050s | Redshift 9x |

**Key insight:** Redshift has a cold start penalty on first query after idle
(~25s overhead). Once warm, Redshift outperforms Snowflake on every query
pattern tested.

---

## Documents in This Folder

| File | What it covers |
|------|---------------|
| [01_parallel_architecture.md](./01_parallel_architecture.md) | How both engines connect to S3 Iceberg — External Volume vs Spectrum |
| [02_materialization_decisions.md](./02_materialization_decisions.md) | Why staging differs across engines — Jinja conditionals + syntax fixes |
| [03_benchmark_queries.md](./03_benchmark_queries.md) | The 5 benchmark queries + setup instructions |
| [04_benchmark_results.md](./04_benchmark_results.md) | Full results table + analysis + conclusions |
| [05_lessons_learned.md](./05_lessons_learned.md) | What broke, what we'd tune, what we'd do differently |

---

## Stack

| Layer | Technology |
|-------|-----------|
| Source format | Apache Iceberg V2 |
| Storage | Amazon S3 |
| Catalog | AWS Glue Data Catalog |
| Snowflake access | External Volume + Catalog Integration |
| Redshift access | Spectrum + External Schema |
| Transformation | dbt Core 1.11.7 |
| Orchestration | Apache Airflow |
| Dataset | NYC Taxi 2024 — 41,169,300 records |

---

## Run Date
April 12, 2026
