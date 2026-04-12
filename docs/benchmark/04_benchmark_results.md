# 04 — Benchmark Results
## ZenClarity UrbanFlow V2 — Snowflake vs Redshift Benchmark Series

---

## Overview
Performance benchmark comparing Snowflake X-Small warehouse against
Redshift Serverless 8 RPU on five identical queries against the same
Apache Iceberg dataset via AWS Glue Catalog.

**Run date:** April 12, 2026
**Dataset:** NYC Taxi trip records 2024 — 41,169,300 records
**Tool:** DBeaver — execution time recorded from results panel
**Queries:** See 03_benchmark_queries.md for full SQL

---

## dbt Build Performance

| Metric | Snowflake XS | Redshift 8 RPU | Delta |
|--------|-------------|----------------|-------|
| Full build time | 14.65s | 36.93s | Snowflake 2.5x faster |
| Models + tests | 67/67 pass | 67/67 pass | Equal |
| Staging build cost | ~0s (views) | ~6s (incremental) | Snowflake cheaper |
| Incremental run | ~2s | ~5s | Snowflake faster |

---

## Query Results

| Query | Description | Rows | SF Cold | SF Warm | RS Cold | RS Warm |
|-------|-------------|------|---------|---------|---------|---------|
| Q1 | Full aggregate scan | 4 | 2.105s | 0.461s | 26.000s | 0.050s |
| Q2 | Date filter — Jan 2024 | 31 | 0.885s | 0.142s | 0.451s | 0.048s |
| Q3 | Join — fact + dim_taxi_zone | 8 | 1.350s | 0.507s | 0.844s | 0.054s |
| Q4 | DQ layer summary | 4 | 2.586s | 0.230s | 1.557s | 0.058s |
| Q5 | Time of day analysis | 5 | 1.009s | 0.104s | 0.660s | 0.064s |

---

## Cold Run Winner

| Query | Winner | Margin |
|-------|--------|--------|
| Q1 — Full scan | Snowflake | 12x faster |
| Q2 — Date filter | Redshift | 2x faster |
| Q3 — Join | Redshift | 1.6x faster |
| Q4 — DQ layer | Redshift | 1.7x faster |
| Q5 — Time of day | Redshift | 1.5x faster |

---

## Warm Run Winner

| Query | Winner | Margin |
|-------|--------|--------|
| Q1 — Full scan | Redshift | 9x faster |
| Q2 — Date filter | Redshift | 3x faster |
| Q3 — Join | Redshift | 9x faster |
| Q4 — DQ layer | Redshift | 4x faster |
| Q5 — Time of day | Redshift | 1.6x faster |

---

## Analysis

### Finding 1 — Cold Start Penalty
Redshift Q1 cold = 26.0s vs Snowflake 2.1s — a 12x difference.
This is the Redshift Serverless cold start penalty. When the workgroup
has been idle, the first query spins up compute before executing —
adding approximately 25 seconds of overhead.

Snowflake does not have this penalty — the XS warehouse stays warm
between queries on the same session.

Mitigation in production: configure a baseline RPU on Redshift
Serverless to keep compute warm during business hours.

### Finding 2 — Warm Performance
Once warm, Redshift outperforms Snowflake on every query. Sub-100ms
on all five warm runs — consistent result-level caching behavior.
Snowflake warm times range from 104ms to 507ms.

### Finding 3 — Filter Pushdown
Q2 cold: Redshift 0.451s vs Snowflake 0.885s — Redshift 2x faster.
The incremental staging table with sort key on ingestion_ts enables
faster partition elimination on filtered date-range scans.

### Finding 4 — Full Scan Cold
Q1 is the only cold query where Snowflake wins. Snowflake's columnar
micro-partition architecture handles unfiltered full-table aggregations
more efficiently on first run at XS tier.

### Finding 5 — Cache Behavior
Redshift result cache is more aggressive — identical repeated queries
return sub-65ms consistently. Snowflake caches effectively but with
higher variance across query patterns.

---

## Conclusions

### Use Snowflake when:
- Pipeline build speed matters — 14.65s vs 36.93s full build
- Cold start latency is unacceptable for the workload
- Staging as zero-cost views reduces compute and storage cost
- Multi-cloud or non-AWS environment required

### Use Redshift when:
- Workload is warm repeated BI dashboard queries
- Filtered date-range scans dominate the access pattern
- Already in AWS ecosystem — no cross-cloud data transfer
- Sub-100ms warm query SLA is required
- Storage cost at scale is the priority

---

## Compute Configuration

| Engine | Config | Hourly Cost |
|--------|--------|-------------|
| Snowflake | X-Small warehouse (1 credit/hr) | ~$2.00/hr |
| Redshift | Serverless 8 RPU | $2.88/hr |

Both at minimum viable compute tier.
Not a maximum throughput test — a cost-performance comparison.
