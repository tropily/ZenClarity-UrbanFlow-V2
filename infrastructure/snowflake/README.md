# Snowflake Infrastructure

## urbanflow_v2_snowflake_setup.sql
Full Snowflake setup for UrbanFlow V2 Iceberg integration.

Establishes:
- Storage Integration → S3 access
- Catalog Integration → Glue metadata access  
- External Volume     → Iceberg read layer
- Iceberg Tables      → TRIP_DATA, TAXI_ZONE_LOOKUP
- RBAC Grants         → DBT_DEV_ROLE least-privilege

## Prerequisites
- teo_snowflake_iceberg_role must exist in AWS IAM
- IAM trust policy must be updated after running DESC commands
- See: docs/runbooks/iceberg_migration_plan.md