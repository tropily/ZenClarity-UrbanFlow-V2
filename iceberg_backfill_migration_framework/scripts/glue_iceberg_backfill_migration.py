import sys
import boto3
from datetime import datetime
from typing import Optional

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.job import Job

# ==============================
# 1. Arguments
# ==============================
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "cab_type",
    "backfill_year",
    "backfill_month",
    "backfill_day",
    "database",
    "table",
    "batch_id"         # ← now received from DAG XCom
])

CAB_TYPE = args["cab_type"]
YEAR     = args["backfill_year"]
MONTH    = args["backfill_month"]
DAY      = args["backfill_day"]
DB       = args["database"]
TBL      = args["table"]
BATCH_ID = args["batch_id"]  # ← glue#green#2024_12_ALL#abc123

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
def build_spark():
    sc           = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session
    job          = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    spark.conf.set("spark.sql.catalog.glue_catalog",
                   "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse",
                   f"s3://{BUCKET}/warehouse/")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
                   "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl",
                   "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark, job

# ==============================
# 4. Schema Alignment
# ==============================
def align_schema(df):
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
        F.lit(BATCH_ID).alias("migration_batch_id"),
        F.lit("GLUE_4.0_NETPLAY").alias("engine_type")
    )

# ==============================
# 5. Main Execution
# ==============================
def main():
    print(f"🚀 GLUE START | batch={BATCH_ID} | path={S3_PATH}")

    try:
        spark, job = build_spark()

        raw_df    = spark.read.parquet(S3_PATH)
        final_df  = align_schema(raw_df)

        # Count before write — forces schema validation before committing
        record_count = final_df.count()
        print(f"🔥 Landing {record_count} records → {ICEBERG_TABLE_STAGE}")

        final_df.writeTo(ICEBERG_TABLE_STAGE).overwritePartitions()

        # Commit Glue job — signals success to Glue console
        job.commit()
        print(f"✅ GLUE SUCCESS | {record_count} records | batch={BATCH_ID}")

    except Exception as e:
        print(f"❌ GLUE FAILURE | batch={BATCH_ID} | error={str(e)}")
        raise  # Re-raise so Glue marks job FAILED in console
               # DAG on_failure_callback handles audit write

if __name__ == "__main__":
    main()
