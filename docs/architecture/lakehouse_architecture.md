flowchart LR

  %% Sources
  subgraph SRC[Sources]
    NYC["NYC Taxi Data (CSV, Parquet)"]
    STREAM_SRC["Simulated Trip Events (JSON)"]
  end

  %% Lakehouse on S3 + Iceberg
  subgraph LAKE[Lakehouse on S3 + Iceberg]
    subgraph BATCH[Batch Ingestion]
      GLUE["Glue Job"]
      SPARK["EMR Spark Job"]
    end

    RAW_S3["raw (S3)"]
    PROC_S3["processed (S3)"]
    WH_S3["warehouse (Iceberg tables)"]
  end

  %% Streaming Ingestion
  subgraph STR[Streaming Ingestion]
    KIN["Kinesis Data Stream"]
    FH["Firehose Delivery Stream"]
    SNOWPIPE["Snowpipe Streaming"]
    SF_STREAM_TBL["Snowflake Streaming Table"]
  end

  %% Query Engines / Warehouses
  subgraph ENGINES[Query Engines / Warehouses]
    SF["Snowflake (External Volume + Tables)"]
    RS["Redshift Spectrum (External Schema)"]
    EMRSPARK["EMR Spark SQL"]
  end

  %% dbt Semantic Layer
  subgraph DBT[dbt Semantic Layer]
    STG["staging models"]
    INT["intermediate models"]
    MARTS["marts (dim_* + fact_trip)"]
    DQ["core / quarantine / steward"]
  end

  %% Observability
  subgraph OBS[Observability]
    AUDIT["AuditClient Library"]
    DDB_REG["ingestion_registry (DynamoDB)"]
    DDB_RUN["pipeline_runs (DynamoDB)"]
  end

  %% Orchestration
  subgraph ORCH[Airflow Orchestration]
    DAG["DAG: sensors + branches + dbt triggers + DDB updates"]
  end

  %% Analytics
  subgraph BI[Analytics]
    STDL["Streamlit Dashboard"]
    TBL["Tableau Workbook"]
  end

  %% Batch flows
  NYC --> GLUE
  NYC --> SPARK

  GLUE --> RAW_S3
  SPARK --> RAW_S3
  RAW_S3 --> PROC_S3 --> WH_S3

  %% Lakehouse -> engines
  WH_S3 --> SF
  WH_S3 --> RS
  WH_S3 --> EMRSPARK

  %% Streaming flows
  STREAM_SRC --> KIN --> FH --> SNOWPIPE --> SF_STREAM_TBL

  %% Engines/dbt
  WH_S3 --> STG
  SF_STREAM_TBL --> STG

  STG --> INT --> MARTS --> DQ

  %% Analytics
  DQ --> STDL
  DQ --> TBL

  %% Observability
  GLUE --> AUDIT
  SPARK --> AUDIT
  SNOWPIPE --> AUDIT
  DAG --> AUDIT

  AUDIT --> DDB_REG
  AUDIT --> DDB_RUN

  %% Orchestration targets
  DAG --> GLUE
  DAG --> SPARK
  DAG --> STG
  DAG --> SF_STREAM_TBL
