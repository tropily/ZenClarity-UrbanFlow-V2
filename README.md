# ZenClarity-UrbanFlow V2 — Architecture Blueprint

> High-level architecture for a **modern, multi-warehouse lakehouse** built on **Iceberg + S3**, **Snowflake**, **Redshift Spectrum**, **Spark/Glue**, **dbt**, **DynamoDB observability**, **Airflow orchestration**, and **Streamlit/Tableau**.

This blueprint is the **source of truth** for how all components fit together.  
Implementation details live in code, but **design intent lives here**.

---

## 1. High-Level Overview

At a high level, ZenClarity-UrbanFlow V2 does the following:

1. **Ingests NYC Taxi trip data** in both **batch** and **streaming** modes.
2. Writes data into an **Iceberg lakehouse on S3** (with Glue Catalog).
3. Exposes the same Iceberg tables to:
   - **Snowflake** via External Volume / External Tables
   - **Redshift Spectrum** via External Schema
   - **EMR/Spark** directly via Iceberg
4. Uses **dbt** to build a **semantic layer**:
   - `staging` → `intermediate` → `marts`
   - `fact_trip` + `dim_*` models
   - **DQ layers**: `core`, `quarantine`, `steward`
5. Tracks ingestion + pipeline health using **DynamoDB** and an **AuditClient** library.
6. Orchestrates the end-to-end flow via **Airflow DAGs**.
7. Surfaces final metrics and insights via **Streamlit** and **Tableau**.
8. Runs a **benchmark suite** to compare performance and cost across engines.

---

## 2. Architecture Diagram (Conceptual)

This Mermaid diagram captures the core data paths and major components.

```mermaid
flowchart LR
  %% Sources
  subgraph SRC[Sources]
    NYC[NYC Taxi Data\n(CSV/Parquet)]
    STREAM[Simulated Trip Events\n(JSON)]
  end

  %% Lakehouse
  subgraph LAKE[Lakehouse on S3 + Iceberg]
    RAW_S3[raw/ (S3)]
    PROC_S3[processed/ (S3)]
    WH_S3[warehouse/ (Iceberg tables)]
  end

  %% Batch Ingestion
  subgraph BATCH[Batch Ingestion]
    GLUE[Glue Job]
    SPARK[EMR Spark Job]
  end

  %% Streaming
  subgraph STR[Streaming Ingestion]
    KIN[Kinesis Data Stream]
    FH[Firehose Delivery Stream]
    SNOWPIPE[Snowpipe Streaming]
    SF_STREAM_TBL[Snowflake\nStreaming Table]
  end

  %% Warehouses & Engines
  subgraph ENGINES[Query Engines / Warehouses]
    SF[Snowflake\n(External Volume + Tables)]
    RS[Redshift Spectrum\n(External Schema)]
    EMRSPARK[EMR Spark SQL]
  end

  %% dbt Layer
  subgraph DBT[dbt Semantic Layer]
    STG[staging models]
    INT[intermediate models]
    MARTS[marts\n(dim_* + fact_trip)]
    DQ[core / quarantine / steward]
  end

  %% Observability
  subgraph OBS[Observability]
    DDB_REG[ingestion_registry\n(DynamoDB)]
    DDB_RUN[pipeline_runs\n(DynamoDB)]
    AUDIT[AuditClient Library]
  end

  %% Orchestration
  subgraph ORCH[Airflow Orchestration]
    DAG[DAG: sensors + branches\n+ dbt triggers + DDB updates]
  end

  %% Analytics
  subgraph BI[Analytics]
    STDL[Streamlit Dashboard]
    TBL[Tableau Workbook]
  end

  %% Flows
  NYC --> GLUE
  NYC --> SPARK

  GLUE --> RAW_S3
  SPARK --> RAW_S3
  RAW_S3 --> PROC_S3 --> WH_S3

  WH_S3 --> SF
  WH_S3 --> RS
  WH_S3 --> EMRSPARK

  STREAM --> KIN --> FH --> SNOWPIPE --> SF_STREAM_TBL

  SF_STREAM_TBL --> STG
  WH_S3 --> STG
  STG --> INT --> MARTS --> DQ

  GLUE --> AUDIT
  SPARK --> AUDIT
  SNOWPIPE --> AUDIT
  DAG --> AUDIT

  AUDIT --> DDB_REG
  AUDIT --> DDB_RUN

  DAG --> GLUE
  DAG --> SPARK
  DAG --> DBT
  DAG --> SF_STREAM_TBL

  MARTS --> STDL
  MARTS --> TBL
