# Start from the official Airflow image
ARG AIRFLOW_VERSION=2.8.1
FROM apache/airflow:${AIRFLOW_VERSION}

# Install the AWS provider and libraries to handle parquet file download/upload
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    pandas \
    fsspec \
    s3fs
