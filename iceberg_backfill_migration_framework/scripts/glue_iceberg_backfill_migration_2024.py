import sys
from typing import Optional

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ==============================
# 1. Job Arguments (Year/Month/Day/Limit)
# ==============================

def _parse_optional_int(val: Optional[str]) -> Optional[int]:
    """
    Convert a Glue arg string to int or None.
    Accepts: "5" -> 5, "None"/""/None -> None
    """
    if val is None:
        return None
    v = val.strip()
    if v == "" or v.lower() == "none":
        return None
    return int(v)

# Define the arguments we expect from Glue
# You can set defaults in the Glue console
args = getResolvedOptions(sys.argv, [
    "BACKFILL_YEAR",
    "BACKFILL_MONTH",
    "BACKFILL_DAY",
    "ROW_LIMIT"
])

BACKFILL_YEAR = int(args["BACKFILL_YEAR"])

BACKFILL_MONTH = _parse_optional_int(args.get("BACKFILL_MONTH"))
BACKFILL_DAY   = _parse_optional_int(args.get("BACKFILL_DAY"))
ROW_LIMIT      = _parse_optional_int(args.get("ROW_LIMIT"))


# Cab types in scope for V2
CAB_TYPES = ["yellow", "green"]

# ==============================
# 2. Static Config
# ==============================

BUCKET = "teo-nyc-taxi"

# Read from Glue table (processed layer)
SRC_TABLE = "teo_nyc_taxi_db.trip_data"

# Iceberg tables in Glue catalog
ICEBERG_TABLE_PROD = "glue_catalog.nyc_taxi_wh.trip_data"          # reference only (not written)
ICEBERG_TABLE_STAGE = "glue_catalog.nyc_taxi_wh.trip_data_v2_stage"
ICEBERG_STAGE_LOCATION = f"s3://{BUCKET}/warehouse/nyc_taxi_wh/trip_data_v2_stage"


# ==============================
# 3. Helpers
# ==============================

def build_spark():
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Catalog config – adjust catalog name if yours differs
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/warehouse/")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

    # Parquet/Timestamp compatibility settings
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
        print(f"Stage table {ICEBERG_TABLE_STAGE} exists, proceeding with write.")
    except Exception as e:
        raise RuntimeError(
            f"Stage table {ICEBERG_TABLE_STAGE} not accessible from Spark. "
            f"Make sure it's created in Glue/Athena using CTAS and that the "
            f"Glue job has the correct Iceberg/Glue catalog config. "
            f"Underlying error: {e}"
        )


def read_processed_for_cab_type(spark, cab_type: str):
    """
    Read from Glue table teo_nyc_taxi_db.trip_data for a cab_type
    and the configured BACKFILL_YEAR/BACKFILL_MONTH/BACKFILL_DAY.
    """
    df = spark.table(SRC_TABLE)

    # Base filters: cab_type, year
    df = df.filter(
        (F.col("cab_type") == F.lit(cab_type)) &
        (F.col("year") == F.lit(BACKFILL_YEAR))
    )

    # Optional month filter (for monthly or daily runs)
    if BACKFILL_MONTH is not None:
        df = df.filter(F.col("month") == F.lit(BACKFILL_MONTH))

    # Optional day filter (for 1-day slice)
    if BACKFILL_DAY is not None:
        df = df.filter(F.col("day") == F.lit(BACKFILL_DAY))

    # Extra safety: year from timestamp
    df = df.filter(F.year("pickup_datetime") == BACKFILL_YEAR)

    print(
        f"Read from {SRC_TABLE} for cab_type={cab_type}, "
        f"year={BACKFILL_YEAR}, month={BACKFILL_MONTH}, day={BACKFILL_DAY}"
    )

    return df


def align_to_iceberg_schema(df, limit_rows: Optional[int] = None):
    """
    Align to nyc_taxi_wh.trip_data_v2_stage schema.
    """
    cols = df.columns

    # Possibly-missing columns with proper aliases
    if "ehail_fee" in cols:
        ehail_fee_col = F.col("ehail_fee").cast("double").alias("ehail_fee")
    else:
        ehail_fee_col = F.lit(None).cast("double").alias("ehail_fee")

    if "trip_type" in cols:
        trip_type_col = F.col("trip_type").cast("bigint").alias("trip_type")
    else:
        trip_type_col = F.lit(None).cast("bigint").alias("trip_type")

    if "congestion_surcharge" in cols:
        congestion_surcharge_col = (
            F.col("congestion_surcharge").cast("double").alias("congestion_surcharge")
        )
    else:
        congestion_surcharge_col = (
            F.lit(None).cast("double").alias("congestion_surcharge")
        )

    if "airport_fee" in cols:
        airport_fee_col = F.col("airport_fee").cast("double").alias("airport_fee")
    else:
        airport_fee_col = F.lit(None).cast("double").alias("airport_fee")

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
            F.current_timestamp().cast("timestamp").alias("ingestion_ts"),
        )
        .filter(F.year("pickup_datetime") == BACKFILL_YEAR)
    )

    if limit_rows is not None and limit_rows > 0:
        aligned = aligned.limit(limit_rows)

    return aligned


def write_to_stage(unified_df):
    ordered_df = unified_df.select(
        "vendorid",
        "cab_type",
        "pickup_datetime",
        "dropoff_datetime",
        "store_and_fwd_flag",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
        "airport_fee",
        "ingestion_ts",
    )

    print(f"Writing to Iceberg table {ICEBERG_TABLE_STAGE} via DataFrame writer")
    (
        ordered_df
        .writeTo(ICEBERG_TABLE_STAGE)
        .overwritePartitions()   # overwrite partitions that appear in this slice
    )


def validate_slice_counts(spark):
    """
    Validation for the configured slice (year/month/day).
    """
    print(f"=== Validation for slice: {describe_slice()} ===")

    src_df = spark.table(SRC_TABLE).filter(
        (F.col("year") == BACKFILL_YEAR) &
        (F.col("cab_type").isin(CAB_TYPES))
    )

    tgt_df = spark.table(ICEBERG_TABLE_STAGE).filter(
        F.year("pickup_datetime") == BACKFILL_YEAR
    ).filter(F.col("cab_type").isin(CAB_TYPES))

    if BACKFILL_MONTH is not None:
        src_df = src_df.filter(F.col("month") == BACKFILL_MONTH)
        tgt_df = tgt_df.filter(F.month("pickup_datetime") == BACKFILL_MONTH)

    if BACKFILL_DAY is not None:
        src_df = src_df.filter(F.col("day") == BACKFILL_DAY)
        tgt_df = tgt_df.filter(
            F.to_date("pickup_datetime") ==
            F.to_date(F.lit(f"{BACKFILL_YEAR:04d}-{BACKFILL_MONTH:02d}-{BACKFILL_DAY:02d}"))
        )

    print("Source counts by cab_type:")
    src_df.groupBy("cab_type").count().show()

    print("Target counts by cab_type:")
    tgt_df.groupBy("cab_type").count().show()

    print("Source total_amount by cab_type:")
    src_df.groupBy("cab_type").agg(F.sum("total_amount").alias("total_amount_sum")).show()

    print("Target total_amount by cab_type:")
    tgt_df.groupBy("cab_type").agg(F.sum("total_amount").alias("total_amount_sum")).show()

    print("Source pickup_datetime range:")
    src_df.agg(
        F.min("pickup_datetime").alias("min_pickup"),
        F.max("pickup_datetime").alias("max_pickup"),
    ).show(truncate=False)

    print("Target pickup_datetime range:")
    tgt_df.agg(
        F.min("pickup_datetime").alias("min_pickup"),
        F.max("pickup_datetime").alias("max_pickup"),
    ).show(truncate=False)


# ==============================
# 4. Main
# ==============================

def main():
    print(
        f"Starting Glue Iceberg backfill for slice: {describe_slice()} "
        f"(ROW_LIMIT={ROW_LIMIT}, CAB_TYPES={CAB_TYPES})"
    )

    spark = build_spark()

    # 1) Ensure stage table exists
    ensure_stage_table_exists(spark)

    # 2) Read from processed yellow & green for the configured slice
    dfs = []
    for cab in CAB_TYPES:
        df_cab = read_processed_for_cab_type(spark, cab)
        print(f"{cab}_proc schema:")
        df_cab.printSchema()
        dfs.append(df_cab)

    unified_src = dfs[0]
    for df_other in dfs[1:]:
        unified_src = unified_src.unionByName(df_other)

    # 3) Align to Iceberg schema (stage table schema)
    aligned_df = align_to_iceberg_schema(unified_src, limit_rows=ROW_LIMIT)
    print("aligned_df schema after align:")
    aligned_df.printSchema()

    count = aligned_df.count()
    print("aligned_df count:", count)

    # 4) Write to stage table
    write_to_stage(aligned_df)

    print(f"Backfill into trip_data_v2_stage completed for slice: {describe_slice()}")

    # 5) Validation in Spark
    validate_slice_counts(spark)


if __name__ == "__main__":
    main()
