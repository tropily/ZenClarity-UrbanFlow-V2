import sys
from typing import Optional

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ==============================
# 1. Job Arguments & Setup
# ==============================

def _parse_optional_int(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    v = val.strip()
    if v == "" or v.lower() == "none":
        return None
    return int(v)

# Resolved options must match the "Job Parameters" in the Glue Console
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BACKFILL_YEAR",
    "BACKFILL_MONTH",
    "BACKFILL_DAY",
    "ROW_LIMIT",
    "BATCH_ID"
])

BACKFILL_YEAR = int(args["BACKFILL_YEAR"])
BACKFILL_MONTH = _parse_optional_int(args.get("BACKFILL_MONTH"))
BACKFILL_DAY   = _parse_optional_int(args.get("BACKFILL_DAY"))
ROW_LIMIT      = _parse_optional_int(args.get("ROW_LIMIT"))
BATCH_ID       = args.get("BATCH_ID", "MANUAL_GLUE_RUN")

CAB_TYPES = ["yellow", "green"]

# ==============================
# 2. Static Config
# ==============================

BUCKET = "teo-nyc-taxi"
SRC_TABLE = "teo_nyc_taxi_db.trip_data"
ICEBERG_TABLE_STAGE = "glue_catalog.nyc_taxi_wh.trip_data_v2_stage"

# ==============================
# 3. Helpers & Business Logic
# ==============================

def build_spark():
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Iceberg/Glue Catalog Configurations
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/warehouse/")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    
    # Parquet/Timestamp Compatibility
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")

    return spark

def describe_slice() -> str:
    if BACKFILL_DAY is not None and BACKFILL_MONTH is not None:
        return f"date={BACKFILL_YEAR:04d}-{BACKFILL_MONTH:02d}-{BACKFILL_DAY:02d}"
    if BACKFILL_MONTH is not None:
        return f"month={BACKFILL_YEAR:04d}-{BACKFILL_MONTH:02d}"
    return f"year={BACKFILL_YEAR:04d}"

def ensure_stage_table_exists(spark):
    try:
        spark.table(ICEBERG_TABLE_STAGE).limit(1).collect()
        print(f"✅ Stage table {ICEBERG_TABLE_STAGE} verified.")
    except Exception as e:
        raise RuntimeError(f"❌ Stage table access failed: {e}")

def read_processed_for_cab_type(spark, cab_type: str):
    df = spark.table(SRC_TABLE)
    df = df.filter(
        (F.col("cab_type") == F.lit(cab_type)) & 
        (F.col("year") == F.lit(BACKFILL_YEAR))
    )
    if BACKFILL_MONTH is not None:
        df = df.filter(F.col("month") == F.lit(BACKFILL_MONTH))
    if BACKFILL_DAY is not None:
        df = df.filter(F.col("day") == F.lit(BACKFILL_DAY))
    
    # Extra safety filter on actual timestamp column
    df = df.filter(F.year("pickup_datetime") == BACKFILL_YEAR)
    return df

def align_to_iceberg_schema(df, limit_rows: Optional[int] = None):
    cols = df.columns
    
    # Define logic for columns that might be missing in older Parquet files
    ehail_fee_col = F.col("ehail_fee").cast("double").alias("ehail_fee") if "ehail_fee" in cols else F.lit(None).cast("double").alias("ehail_fee")
    trip_type_col = F.col("trip_type").cast("bigint").alias("trip_type") if "trip_type" in cols else F.lit(None).cast("bigint").alias("trip_type")
    congestion_surcharge_col = F.col("congestion_surcharge").cast("double").alias("congestion_surcharge") if "congestion_surcharge" in cols else F.lit(None).cast("double").alias("congestion_surcharge")
    airport_fee_col = F.col("airport_fee").cast("double").alias("airport_fee") if "airport_fee" in cols else F.lit(None).cast("double").alias("airport_fee")

    aligned = (
        df
        .select(
            F.col("vendorid").cast("int").alias("vendorid"),
            F.col("cab_type").cast("string").alias("cab_type"),
            F.col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            F.col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
            F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
            F.col("ratecodeid").cast("bigint").alias("ratecodeid"),
            F.col("pulocationid").cast("int").alias("pulocationid"),
            F.col("dolocationid").cast("int").alias("dolocationid"),
            F.col("passenger_count").cast("bigint").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("extra").cast("double").alias("extra"),
            F.col("mta_tax").cast("double").alias("mta_tax"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("tolls_amount").cast("double").alias("tolls_amount"),
            ehail_fee_col,
            F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
            F.col("total_amount").cast("double").alias("total_amount"),
            F.col("payment_type").cast("bigint").alias("payment_type"),
            trip_type_col,
            congestion_surcharge_col,
            airport_fee_col,
            
            # === NEW AUDIT COLUMNS (FOR 'PRO' LINEAGE) ===
            F.lit(BATCH_ID).alias("migration_batch_id"),
            F.lit("GLUE_4.0").alias("engine_type"),
            F.current_timestamp().cast("timestamp").alias("ingestion_ts")
        )
        .filter(F.year("pickup_datetime") == BACKFILL_YEAR)
    )

    if limit_rows is not None and limit_rows > 0:
        aligned = aligned.limit(limit_rows)
    return aligned

def write_to_stage(unified_df):
    """
    Final sink into Iceberg. We use overwritePartitions to ensure 
    re-runs for the same month clean up previous attempts.
    """
    ordered_df = unified_df.select(
        "vendorid", "cab_type", "pickup_datetime", "dropoff_datetime",
        "store_and_fwd_flag", "ratecodeid", "pulocationid", "dolocationid",
        "passenger_count", "trip_distance", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",
        "improvement_surcharge", "total_amount", "payment_type", "trip_type",
        "congestion_surcharge", "airport_fee", 
        "migration_batch_id", "engine_type", "ingestion_ts"
    )

    print(f"🚀 Writing slice to Iceberg: {ICEBERG_TABLE_STAGE}")
    ordered_df.writeTo(ICEBERG_TABLE_STAGE).overwritePartitions()

# ==============================
# 4. Main Execution Path
# ==============================

def main():
    print(f"--- MIGRATION START: {describe_slice()} | BATCH: {BATCH_ID} ---")
    
    spark = build_spark()
    ensure_stage_table_exists(spark)

    # Union the Processed Layers (Yellow + Green)
    dfs = [read_processed_for_cab_type(spark, cab) for cab in CAB_TYPES]
    unified_src = dfs[0]
    for df_other in dfs[1:]:
        unified_src = unified_src.unionByName(df_other)

    # Align Schema and Inject Lineage Metadata
    aligned_df = align_to_iceberg_schema(unified_src, limit_rows=ROW_LIMIT)
    
    # === TELEMETRY SIGNAL: CAPTURE COUNT BEFORE SINK ===
    final_count = aligned_df.count()
    print(f"METRIC_RECORD_COUNT: {final_count}")
    print(f"METRIC_BATCH_ID: {BATCH_ID}")

    # Land the data
    write_to_stage(aligned_df)
    
    print(f"✅ MIGRATION SUCCESS: {describe_slice()} is LANDED.")

if __name__ == "__main__":
    main()