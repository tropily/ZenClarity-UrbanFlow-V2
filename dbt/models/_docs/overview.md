{% docs overview %}
# 🌆 ZenClarity-UrbanFlow — Overview

**Modern DE project:** streaming + batch ingestion (AWS), dbt modeling (staging → intermediate → marts), and cross-warehouse benchmarks (Redshift vs Snowflake). Streamlit surfaces KPIs.

**Highlights**
- Dual pipelines: Kinesis Firehose (streaming) + Glue (batch) → S3
- dbt models with portable macros across Redshift/Snowflake
- Dashboard-ready marts (agg_* models) with tests & lineage
- Exposures: Streamlit app and benchmark pack

**Repo:** https://github.com/tropily/ZenClarity-UrbanFlow
{% enddocs %}
