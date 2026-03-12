-- URBANFLOW V2 — SNOWFLAKE ICEBERG SETUP
-- Author  : Nguyen Le | ZenClarity Consulting
-- Project : UrbanFlow V2 — NYC Taxi Pipeline Migration
-- Updated : March 2026
-- ------------------------------------------------------------
-- PURPOSE:
--   This script establishes the full Snowflake foundation for
--   UrbanFlow V2. It connects Snowflake to the Iceberg tables
--   managed by AWS Glue Catalog, sitting on S3 physical files.
--   dbt owns all transformation logic downstream.
--
-- ARCHITECTURE OVERVIEW:
--
--   S3 (teo-nyc-taxi/warehouse/)          ← physical Iceberg files
--        ↓
--   AWS Glue Catalog (nyc_taxi_wh)        ← Iceberg metadata
--        ↓
--   Snowflake Catalog Integration          ← reads Glue metadata
--   Snowflake External Volume             ← reads S3 files
--        ↓
--   NYC_TAXI_DEV.RAW_ICEBERG              ← Iceberg tables in Snowflake
--        ↓
--   dbt: STG → INT → MART                 ← all transformations
--
-- EXECUTION ORDER:
--   Step 0  — Database & Schema setup
--   Step 1  — Storage Integration (S3 access)
--   Step 2  — Catalog Integration (Glue metadata access)
--   Step 3  — External Volume (Iceberg read layer)
--   Step 4  — Iceberg Table definitions
--   Step 5  — RBAC grants for dbt
--   Step 6  — Validation queries
-- ============================================================


-- ============================================================
-- STEP 0 — DATABASE & SCHEMA SETUP
-- ------------------------------------------------------------
-- One database (NYC_TAXI_DEV) with layered schemas following
-- the standard dbt architecture pattern:
--
--   RAW_ICEBERG   → source layer  — Iceberg external tables
--                   dbt reads from here, never writes
--   STG_NYC_TAXI  → staging layer — dbt staging models
--   INT_NYC_TAXI  → intermediate  — dbt intermediate models
--   MART_NYC_TAXI → mart layer    — final analytics tables
--   SANDBOX_VIPER → scratch space — ad hoc dev / testing
-- ============================================================

CREATE DATABASE IF NOT EXISTS NYC_TAXI_DEV;

CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DEV.RAW_ICEBERG
    COMMENT = 'Source layer — Iceberg external tables via Glue catalog. Read only.';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DEV.STG_NYC_TAXI
    COMMENT = 'dbt staging layer — type casting, renaming, basic cleaning';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DEV.INT_NYC_TAXI
    COMMENT = 'dbt intermediate layer — business logic, joins, enrichment';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DEV.MART_NYC_TAXI
    COMMENT = 'dbt mart layer — final analytics tables for consumption';

CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DEV.SANDBOX_VIPER
    COMMENT = 'Personal scratch space — ad hoc queries and development';


-- ============================================================
-- STEP 1 — STORAGE INTEGRATION
-- ------------------------------------------------------------
-- PURPOSE:
--   Establishes the trust relationship between Snowflake and
--   the S3 bucket where Iceberg files physically live.
--   This is the network-level access layer — it answers:
--   "Is Snowflake allowed to read this S3 path?"
--
-- IMPORTANT — AFTER RUNNING THIS STEP:
--   Run: DESC INTEGRATION INTG_NYC_ICEBERG_S3;
--   Copy these two values — you need them for the IAM trust
--   policy on teo_snowflake_iceberg_role in AWS:
--     STORAGE_AWS_IAM_USER_ARN
--     STORAGE_AWS_EXTERNAL_ID
--
-- IAM ROLE: teo_snowflake_iceberg_role
--   Must have S3 read permissions on:
--   s3://teo-nyc-taxi/warehouse/
-- ============================================================

CREATE OR REPLACE STORAGE INTEGRATION INTG_NYC_ICEBERG_S3
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = S3
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::******:role/teo_snowflake_iceberg_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://****/warehouse/');

-- Run this to get the IAM principal values for the trust policy
DESC INTEGRATION INTG_NYC_ICEBERG_S3;

-- RECORD THESE VALUES AFTER RUNNING DESC:
-- STORAGE_AWS_IAM_USER_ARN : "arn:aws:iam::***:user/****-s"
-- STORAGE_AWS_EXTERNAL_ID  : "********"
-- These go into the trust policy of teo_snowflake_iceberg_role in AWS IAM


-- ============================================================
-- STEP 2 — CATALOG INTEGRATION
-- ------------------------------------------------------------
-- PURPOSE:
--   Connects Snowflake to the AWS Glue Data Catalog so it can
--   read Iceberg table metadata — schema, partition info,
--   snapshot history, and file locations.
--   This answers: "What does this Iceberg table look like?"
--
-- NOTE:
--   Separate from Step 1. Step 1 = file access (S3).
--   Step 2 = metadata access (Glue). Both are required.
--
-- IMPORTANT — AFTER RUNNING THIS STEP:
--   Run: DESC CATALOG INTEGRATION INTG_GLUE_NYC_ICEBERG;
--   Copy these values — needed for IAM trust policy:
--     GLUE_AWS_IAM_USER_ARN
--     GLUE_AWS_EXTERNAL_ID
--
-- GLUE CATALOG TARGETS:
--   Database  : nyc_taxi_wh
--   Tables    : trip_data, taxi_zone_lookup
--   Region    : us-east-1
-- ============================================================

CREATE OR REPLACE CATALOG INTEGRATION INTG_GLUE_NYC_ICEBERG
  CATALOG_SOURCE    = GLUE
  TABLE_FORMAT      = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::*****:role/teo_snowflake_iceberg_role'
  GLUE_CATALOG_ID   = '*****'
  GLUE_REGION       = 'us-east-1'
  ENABLED           = TRUE
  COMMENT           = 'Glue catalog integration for NYC Taxi Iceberg tables';

-- Run this to get the IAM principal values for the trust policy
DESC CATALOG INTEGRATION INTG_GLUE_NYC_ICEBERG;

-- RECORD THESE VALUES AFTER RUNNING DESC:
-- GLUE_AWS_IAM_USER_ARN : "*****"
-- GLUE_AWS_EXTERNAL_ID  : "************"


-- ============================================================
-- STEP 3 — EXTERNAL VOLUME
-- ------------------------------------------------------------
-- PURPOSE:
--   The External Volume is the Iceberg-specific read layer.
--   It sits on top of the Storage Integration and tells
--   Snowflake exactly where the Iceberg warehouse root is on S3.
--
-- THINK OF IT AS:
--   Storage Integration = permission to access S3
--   External Volume     = the specific S3 path for Iceberg files
--
-- ALLOW_WRITES = FALSE — intentional.
--   Snowflake is a read-only consumer of this Iceberg data.
--   Glue + EMR/Glue jobs own all writes to the Iceberg table.
--   Preventing writes from Snowflake protects data integrity.
--
-- S3 PATH:
--   s3://*****/warehouse/nyc_taxi_wh/
--   This is where Glue writes the Iceberg metadata + data files
-- ============================================================

CREATE OR REPLACE EXTERNAL VOLUME EV_NYC_ICEBERG
  STORAGE_LOCATIONS = (
    (
      NAME                     = 'nyc_taxi_wh_location'
      STORAGE_PROVIDER         = 'S3'
      STORAGE_BASE_URL         = 's3://****/warehouse/nyc_taxi_wh/'
      STORAGE_AWS_ROLE_ARN     = 'arn:aws:iam::*******:role/teo_snowflake_iceberg_role'
      STORAGE_AWS_EXTERNAL_ID  = 'FVC43135_******'
    )
  )
  ALLOW_WRITES = FALSE;  -- Snowflake is read-only. Glue/EMR own all writes.

-- Verify the external volume is correctly configured
DESC EXTERNAL VOLUME EV_NYC_ICEBERG;

-- End-to-end connectivity test — should return SUCCESS
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('EV_NYC_ICEBERG');


-- ============================================================
-- STEP 4 — ICEBERG TABLE DEFINITIONS
-- ------------------------------------------------------------
-- PURPOSE:
--   Create Snowflake-managed Iceberg table references pointing
--   at the tables registered in the Glue catalog.
--   No data is copied — Snowflake reads directly from S3
--   using the Glue metadata to understand the schema and
--   file locations.
--
-- AUTO_REFRESH = TRUE:
--   Snowflake automatically picks up Iceberg metadata changes
--   (new snapshots, schema evolution) without manual refresh.
--   This means when Glue/EMR writes new data, Snowflake sees
--   it automatically on the next query.
--
-- TABLES:
--   trip_data        → core fact table — all cab types all years
--   taxi_zone_lookup → dimension table — location reference data
-- ============================================================

USE DATABASE NYC_TAXI_DEV;
USE SCHEMA RAW_ICEBERG;

-- ── Trip Data ────────────────────────────────────────────────
-- Source  : nyc_taxi_wh.trip_data in Glue catalog
-- Content : Yellow, Green, FHV, HVFHV trips — 2019 to present
-- Partition: day(pickup_datetime) — set at Iceberg table level
-- Metadata: ingestion_ts, batch_id, engine_type added by Glue/EMR jobs

CREATE OR REPLACE ICEBERG TABLE NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA
  CATALOG            = 'INTG_GLUE_NYC_ICEBERG'
  EXTERNAL_VOLUME    = 'EV_NYC_ICEBERG'
  CATALOG_TABLE_NAME = 'trip_data'
  CATALOG_NAMESPACE  = 'nyc_taxi_wh'
  AUTO_REFRESH       = TRUE
  COMMENT            = 'UrbanFlow V2 — NYC Taxi trips via Glue Iceberg catalog. Read only.';

-- Quick row count validation
SELECT COUNT(*) FROM NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA;

-- ── Taxi Zone Lookup ─────────────────────────────────────────
-- Source  : nyc_taxi_wh.taxi_zone_lookup in Glue catalog
-- Content : Location ID → Borough + Zone + Service Zone mapping
-- Used for joining pickup/dropoff location IDs to readable names

CREATE OR REPLACE ICEBERG TABLE NYC_TAXI_DEV.RAW_ICEBERG.TAXI_ZONE_LOOKUP
  CATALOG            = 'INTG_GLUE_NYC_ICEBERG'
  EXTERNAL_VOLUME    = 'EV_NYC_ICEBERG'
  CATALOG_TABLE_NAME = 'taxi_zone_lookup'
  CATALOG_NAMESPACE  = 'nyc_taxi_wh'
  AUTO_REFRESH       = TRUE
  COMMENT            = 'UrbanFlow V2 — Taxi zone reference data via Glue Iceberg catalog. Read only.';

-- Quick row count validation
SELECT COUNT(*) FROM NYC_TAXI_DEV.RAW_ICEBERG.TAXI_ZONE_LOOKUP;


-- ============================================================
-- STEP 5 — RBAC GRANTS FOR DBT
-- ------------------------------------------------------------
-- PURPOSE:
--   Grant DBT_DEV_ROLE the minimum permissions needed to
--   run dbt models. This follows least-privilege principle:
--
--   DBT_DEV_ROLE CAN:
--     → Read from RAW_ICEBERG (source layer)
--     → Create/replace tables and views in STG, INT, MART
--
--   DBT_DEV_ROLE CANNOT:
--     → Write to RAW_ICEBERG (source layer is read-only)
--     → Access other databases
--     → Modify integrations or volumes
--
-- NOTE ON FUTURE GRANTS:
--   GRANT SELECT ON FUTURE TABLES ensures that when new
--   Iceberg tables are added to RAW_ICEBERG, DBT_DEV_ROLE
--   automatically gets SELECT without needing to re-run grants.
-- ============================================================

-- Database access
GRANT USAGE ON DATABASE NYC_TAXI_DEV
    TO ROLE DBT_DEV_ROLE;

-- Schema access — read layer
GRANT USAGE ON SCHEMA NYC_TAXI_DEV.RAW_ICEBERG
    TO ROLE DBT_DEV_ROLE;

-- Schema access — write layers
GRANT USAGE ON SCHEMA NYC_TAXI_DEV.STG_NYC_TAXI
    TO ROLE DBT_DEV_ROLE;
GRANT USAGE ON SCHEMA NYC_TAXI_DEV.INT_NYC_TAXI
    TO ROLE DBT_DEV_ROLE;
GRANT USAGE ON SCHEMA NYC_TAXI_DEV.MART_NYC_TAXI
    TO ROLE DBT_DEV_ROLE;

-- Read access on RAW_ICEBERG — current and future tables
GRANT SELECT ON ALL TABLES IN SCHEMA NYC_TAXI_DEV.RAW_ICEBERG
    TO ROLE DBT_DEV_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NYC_TAXI_DEV.RAW_ICEBERG
    TO ROLE DBT_DEV_ROLE;

-- Write access — dbt creates tables and views in these schemas
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI_DEV.STG_NYC_TAXI
    TO ROLE DBT_DEV_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI_DEV.INT_NYC_TAXI
    TO ROLE DBT_DEV_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA NYC_TAXI_DEV.MART_NYC_TAXI
    TO ROLE DBT_DEV_ROLE;

-- Sandbox — full access for ad hoc development
GRANT ALL PRIVILEGES ON SCHEMA NYC_TAXI_DEV.SANDBOX_VIPER
    TO ROLE DBT_DEV_ROLE;


-- ============================================================
-- STEP 6 — VALIDATION QUERIES
-- ------------------------------------------------------------
-- PURPOSE:
--   Run these after setup to confirm everything is working
--   end to end before starting dbt work.
-- ============================================================

-- ── 6.1 Row counts per table ─────────────────────────────────
SELECT COUNT(*) AS trip_count
FROM NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA;

SELECT COUNT(*) AS zone_count
FROM NYC_TAXI_DEV.RAW_ICEBERG.TAXI_ZONE_LOOKUP;

-- ── 6.2 Sample rows ──────────────────────────────────────────
SELECT * FROM NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA
LIMIT 1;

SELECT * FROM NYC_TAXI_DEV.RAW_ICEBERG.TAXI_ZONE_LOOKUP
LIMIT 1;

-- ── 6.3 Data distribution by cab type ────────────────────────
-- Confirms all four cab types landed correctly
SELECT
    cab_type,
    COUNT(*)             AS record_count,
    MIN(pickup_datetime) AS earliest_trip,
    MAX(pickup_datetime) AS latest_trip,
    MAX(ingestion_ts)    AS last_ingested
FROM NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA
GROUP BY cab_type
ORDER BY cab_type;

-- ── 6.4 Schema inspection ────────────────────────────────────
DESC ICEBERG TABLE NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA;
DESC ICEBERG TABLE NYC_TAXI_DEV.RAW_ICEBERG.TAXI_ZONE_LOOKUP;

-- ── 6.5 DDL inspection ───────────────────────────────────────
SELECT GET_DDL('table', 'NYC_TAXI_DEV.RAW_ICEBERG.TRIP_DATA');

-- ── 6.6 Column listing with types ────────────────────────────
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION
FROM NYC_TAXI_DEV.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME   = 'TRIP_DATA'
  AND TABLE_SCHEMA = 'RAW_ICEBERG'
ORDER BY ORDINAL_POSITION;
