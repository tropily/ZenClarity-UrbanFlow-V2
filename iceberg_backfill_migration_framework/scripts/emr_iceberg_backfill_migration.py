import sys
import argparse
import boto3
import time
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, functions as F

# ==============================
# 0. Framework & Metadata Setup
# ==============================
try:
    from iceberg_migration_utils import migration_control_tower
except ImportError:
    def migration_control_tower(engine_type="EMR"):
        def decorator(f): return f
        return decorator

# ==============================
# 1. CLI Arguments
# ==============================
parser = argparse.ArgumentParser(description="EMR Spark Multi-Resolution Backfill")
parser.add_argument("--cab-type",       required=True)
parser.add_argument("--backfill-year",  required=True)
parser.add_argument("--backfill-month", default="ALL")
parser.add_argument("--backfill-day",   default="ALL")
parser.add_argument("--batch-id",       default="EMR_VOLUMETRIC_RUN")
parser.add_argument("--database",       required=False, default="default")
parser.add_argument("--table",          required=False, default="temp_table")

cli_args = parser.parse_args()

CAB_TYPE = cli_args.cab_type
YEAR     = cli_args.backfill_year
MONTH    = cli_args.backfill_month
DAY      = cli_args.backfill_day
BATCH_ID = cli_args.batch_id        # ← emr#green#2024_12_ALL#abc123
DB       = cli_args.database
TBL      = cli_args.table

# ==============================
# 2. Dynamic S3 Path
# ==============================
BUCKET = "teo-nyc-taxi"
S3_PATH = f"s3://{BUCKET}/processed/trip_data/cab_type={CAB_TYPE}/year={YEAR}/"

if MONTH != "ALL":
    S3_PATH += f"month={int(MONTH)}/"
    if DAY != "ALL":
        S3_PATH += f"day={int(DAY)}/"

ICEBERG_TABLE_STAGE = f"glue_catalog.nyc_taxi_wh.{TBL}_v2_stage"

# ==============================
# 3. Spark Setup
# ==============================
def build_spark() -> SparkSession:
    spark = (SparkSession.builder
        .appName(f"EMR_{CAB_TYPE}_{YEAR}_{MONTH}_{DAY}")
        .enableHiveSupport()
        .getOrCreate())

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

# ==============================
# 4. Schema Alignment
# ==============================
def align_to_iceberg_schema(df, migration_batch_id: str):
    cols = df.columns
    return df.select(
        F.col("vendorid").cast("int"),
        F.lit(CAB_TYPE).alias("cab_type"),
        F.col("pickup_datetime").cast("timestamp"),
        F.col("dropoff_datetime").cast("timestamp"),
        F.col("store_and_fwd_flag").cast("string"),
        F.col("ratecodeid").cast("bigint"),
        F.col("pulocationid").cast("int"),
        F.col("dolocationid").cast("int"),
        F.col("passenger_count").cast("bigint"),
        F.col("trip_distance").cast("double"),
        F.col("fare_amount").cast("double"),
        F.col("extra").cast("double"),
        F.col("mta_tax").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("tolls_amount").cast("double"),
        (F.col("ehail_fee").cast("double")
            if "ehail_fee" in cols
            else F.lit(None).cast("double")).alias("ehail_fee"),
        F.col("improvement_surcharge").cast("double"),
        F.col("total_amount").cast("double"),
        F.col("payment_type").cast("bigint"),
        (F.col("trip_type").cast("bigint")
            if "trip_type" in cols
            else F.lit(None).cast("bigint")).alias("trip_type"),
        (F.col("congestion_surcharge").cast("double")
            if "congestion_surcharge" in cols
            else F.lit(None).cast("double")).alias("congestion_surcharge"),
        (F.col("airport_fee").cast("double")
            if "airport_fee" in cols
            else F.lit(None).cast("double")).alias("airport_fee"),
        # Metadata
        F.current_timestamp().alias("ingestion_ts"),
        F.lit(BATCH_ID).alias("batch_id"),
        F.lit(S3_PATH).alias("source_file"),
        F.lit(migration_batch_id).alias("migration_batch_id"),
        F.lit("EMR_7.7.0_MULTI_RES").alias("engine_type")
    )

# ==============================
# 5. Main Execution
# ==============================
@migration_control_tower(engine_type="EMR_VOLUMETRIC")
def run_emr_job(session, batch_id):
    start_time = time.time()
    spark      = build_spark()

    try:
        print(f"🚀 EMR START | batch={batch_id} | path={S3_PATH}")

        raw_df     = spark.read.parquet(S3_PATH)
        aligned_df = align_to_iceberg_schema(raw_df, batch_id)

        record_count = aligned_df.count()
        print(f"🔥 Landing {record_count} records → {ICEBERG_TABLE_STAGE}")

        aligned_df.writeTo(ICEBERG_TABLE_STAGE).overwritePartitions()

        duration = time.time() - start_time
        print(f"✅ EMR SUCCESS | {record_count} records | "
              f"{duration:.2f}s | batch={batch_id}")

        return {"record_count": record_count,
                "status":       "SUCCESS",
                "duration":     duration}

    except Exception as e:
        print(f"❌ EMR FAILURE | batch={batch_id} | error={str(e)}")
        raise  # Re-raise so EMR marks step FAILED
               # DAG on_failure_callback handles audit write

if __name__ == "__main__":
    active_session = boto3.Session()
    run_emr_job(session=active_session, batch_id=BATCH_ID)