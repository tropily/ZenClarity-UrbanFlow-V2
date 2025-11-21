from pyspark.sql import SparkSession, functions as F

# -----------------------------
# Config – adjust if needed
# -----------------------------
BUCKET = "teo-nyc-taxi"

PROCESSED_BASE = f"s3://{BUCKET}/processed/trip_data"

# Prod table (reference only – we DO NOT write here in this script)
ICEBERG_TABLE_PROD = "nyc_taxi_wh.trip_data"

# V2 staging table – this script writes ONLY here
ICEBERG_TABLE_STAGE = "nyc_taxi_wh.trip_data_v2_stage"
ICEBERG_STAGE_LOCATION = f"s3://{BUCKET}/warehouse/nyc_taxi_wh/trip_data_v2_stage"

BACKFILL_YEAR = 2024


def build_spark():
    """
    Create a SparkSession with Iceberg + Glue catalog enabled.
    Adjust catalog name/configs to your environment if needed.
    """
    spark = (
        SparkSession.builder
        .appName("iceberg_backfill_2024_v2_stage")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/warehouse/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("USE glue_catalog")
    return spark


def ensure_stage_table_exists(spark):
    """
    Create the V2 stage table if it doesn't exist yet,
    using the prod table schema as a template.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE_STAGE}
        LIKE {ICEBERG_TABLE_PROD}
        LOCATION '{ICEBERG_STAGE_LOCATION}'
    """)


def read_processed_for_cab_type(spark, cab_type: str):
    """
    Read processed trip_data for a given cab_type and BACKFILL_YEAR
    from partitioned S3 paths:

      s3://teo-nyc-taxi/processed/trip_data/cab_type=<cab_type>/year=<year>/...

    Assumes the processed Parquet files already have
    normalized column names close to the Iceberg schema.
    """
    base_path = f"{PROCESSED_BASE}/cab_type={cab_type}/year={BACKFILL_YEAR}"
    df = spark.read.parquet(base_path)

    # Partition columns from the path (cab_type, year, month, day) should be present automatically.
    # We still filter by year() on pickup_datetime to be safe.
    return df.filter(F.year("pickup_datetime") == BACKFILL_YEAR)


def align_to_iceberg_schema(df):
    """
    Align the processed dataframe to the Iceberg trip_data schema.
    We assume processed has these or compatible columns:

      vendorid, cab_type, pickup_datetime, dropoff_datetime,
      store_and_fwd_flag, ratecodeid, pulocationid, dolocationid,
      passenger_count, trip_distance, fare_amount, extra, mta_tax,
      tip_amount, tolls_amount, improvement_surcharge, total_amount,
      payment_type, [trip_type], [congestion_surcharge], [airport_fee]
    """
    cols = df.columns

    aligned = (
        df
        .select(
            F.col("vendorid").cast("int"),
            F.col("cab_type").cast("string"),
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
            # optional columns
            F.col("ehail_fee").cast("double") if "ehail_fee" in cols else F.lit(None).cast("double"),
            F.col("improvement_surcharge").cast("double"),
            F.col("total_amount").cast("double"),
            F.col("payment_type").cast("bigint"),
            F.col("trip_type").cast("bigint") if "trip_type" in cols else F.lit(None).cast("bigint"),
            F.col("congestion_surcharge").cast("double") if "congestion_surcharge" in cols else F.lit(None).cast("double"),
            F.col("airport_fee").cast("double") if "airport_fee" in cols else F.lit(None).cast("double"),
            F.current_timestamp().alias("ingestion_ts").cast("timestamp"),
        )
        # safety filter: only 2024
        .filter(F.year("pickup_datetime") == BACKFILL_YEAR)
        # safety filter: only taxi cab types
        .filter(F.col("cab_type").isin("yellow", "green"))
    )

    return aligned


def main():
    spark = build_spark()

    # 1) Ensure the V2 stage table exists
    ensure_stage_table_exists(spark)

    # 2) Read processed yellow & green for 2024
    yellow_proc = read_processed_for_cab_type(spark, "yellow")
    green_proc  = read_processed_for_cab_type(spark, "green")

    # 3) Align both to the Iceberg trip_data schema
    yellow_df = align_to_iceberg_schema(yellow_proc)
    green_df  = align_to_iceberg_schema(green_proc)

    unified_df = yellow_df.unionByName(green_df)

    unified_df.createOrReplaceTempView("trip_data_2024_v2_stage_tmp")

    # 4) Overwrite the V2 stage table with 2024 data
    #    (This script NEVER writes to prod table.)
    spark.sql(f"""
        INSERT OVERWRITE TABLE {ICEBERG_TABLE_STAGE}
        SELECT *
        FROM trip_data_2024_v2_stage_tmp
    """)

    spark.stop()


if __name__ == "__main__":
    main()
