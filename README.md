## Project Goal

ZenClarity-UrbanFlow V2 is a production-grade lakehouse portfolio demonstrating end-to-end Senior Data Engineering capabilities across AWS, Iceberg, dbt, Snowflake, Redshift Spectrum, Airflow, DynamoDB observability, real-time streaming, and BI.

## Architecture

See the full architecture diagram:  
[docs/architecture/lakehouse_architecture.md](docs/architecture/lakehouse_architecture.md)

## Repository Structure

'''
.
├── README.md
├── dashboards
│   ├── streamlit
│   └── tableau
├── dbt
│   ├── macros
│   ├── models
│   ├── profiles
│   ├── seeds
│   ├── snapshots
│   └── tests
├── diagrams
├── docs
│   ├── architecture
│   ├── benchmark
│   ├── decisions
│   ├── readme_assets
│   └── runbooks
├── infrastructure
│   ├── emr
│   ├── glue
│   ├── iam
│   ├── kinesis
│   ├── redshift
│   ├── snowflake
│   └── terraform
├── lakehouse
│   ├── processed
│   ├── raw
│   ├── streaming
│   ├── validation
│   └── warehouse
└── scripts
    ├── batch
    ├── benchmark
    ├── helpers
    └── streaming
'''