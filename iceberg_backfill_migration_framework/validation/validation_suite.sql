-- ============================================================
-- ATHENA VALIDATION SUITE
-- Validates Glue → Iceberg backfill correctness for year 2024
-- Compares processed parquet table vs. Iceberg stage table
-- ============================================================


-- ============================================================
-- 1. TOTAL ROW COUNT MATCH (2024)
-- ============================================================

-- Parquet (processed)
SELECT COUNT(*) AS processed_count
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024
  AND cab_type IN ('yellow', 'green');

-- Iceberg
SELECT COUNT(*) AS iceberg_count
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024
  AND cab_type IN ('yellow', 'green');



-- ============================================================
-- 2. ROW COUNT MATCH BY CAB TYPE
-- ============================================================

-- Parquet
SELECT cab_type, COUNT(*) AS processed_count
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024
GROUP BY cab_type
ORDER BY cab_type;

-- Iceberg
SELECT cab_type, COUNT(*) AS iceberg_count
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024
GROUP BY cab_type
ORDER BY cab_type;



-- ============================================================
-- 3. ROW COUNT BY MONTH (2024)
-- ============================================================

-- Parquet
SELECT month, COUNT(*) AS processed_count
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024
GROUP BY month
ORDER BY month;

-- Iceberg
SELECT month(pickup_datetime) AS month, COUNT(*) AS iceberg_count
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024
GROUP BY month
ORDER BY month;



-- ============================================================
-- 4. TOTAL AMOUNT SUM CHECK (2024)
-- ============================================================

-- Parquet
SELECT SUM(total_amount) AS processed_total
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024;

-- Iceberg
SELECT SUM(total_amount) AS iceberg_total
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024;



-- ============================================================
-- 5. VALUE DISTRIBUTION CHECK (PULocationID, TOP 20)
-- ============================================================

-- Parquet
SELECT pulocationid, COUNT(*) AS processed_count
FROM teo_nyc_taxi_db.trip_data
WHERE year  = 2024
GROUP BY pulocationid
ORDER BY processed_count DESC
LIMIT 20;

-- Iceberg
SELECT pulocationid, COUNT(*) AS iceberg_count
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024
GROUP BY pulocationid
ORDER BY iceberg_count DESC
LIMIT 20;



-- ============================================================
-- 6. PICKUP_DATETIME RANGE VALIDATION
-- ============================================================

-- Parquet
SELECT MIN(pickup_datetime) AS min_processed,
       MAX(pickup_datetime) AS max_processed
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024;

-- Iceberg
SELECT MIN(pickup_datetime) AS min_iceberg,
       MAX(pickup_datetime) AS max_iceberg
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024;



-- ============================================================
-- 7. RANDOM SPOT CHECK ON SAMPLE DAY (2024-06-15)
-- ============================================================

-- Parquet
SELECT COUNT(*) AS processed_count
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024 AND month = 6 AND day = 15;

-- Iceberg
SELECT COUNT(*) AS iceberg_count
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE pickup_datetime BETWEEN TIMESTAMP '2024-06-15 00:00:00'
                          AND TIMESTAMP '2024-06-16 00:00:00';



-- ============================================================
-- 8. DEEP CHECK: MATCH TOP 10 RECORDS BY FARE
-- (Ensures schema mapping & data integrity)
-- ============================================================

-- Parquet
SELECT vendorid, pulocationid, total_amount, pickup_datetime
FROM teo_nyc_taxi_db.trip_data
WHERE year = 2024
ORDER BY total_amount DESC
LIMIT 10;

-- Iceberg
SELECT vendorid, pulocationid, total_amount, pickup_datetime
FROM nyc_taxi_wh.trip_data_v2_stage
WHERE year(pickup_datetime) = 2024
ORDER BY total_amount DESC
LIMIT 10;
