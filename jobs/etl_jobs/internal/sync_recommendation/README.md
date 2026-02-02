# Sync Recommendation Tables

This job is used to sync the recommendation tables between BigQuery and Cloud SQL.

We use DuckDB to handle the processing between GCS and CloudSQL directly in memory.

## How to install and run the job

```bash
uv pip install -r requirements.txt
```

## BigQuery to Cloud SQL

1. Export the data from BigQuery to GCS

    ```bash
    python bq_to_sql.py bq-to-gcs \
        --table-name user_statistics \
        --bucket-path gs://data-bucket-dev/export/bigquery_to_cloudsql_recommendation_tables/20240414_100000/ \
        --date 20240414
    ```

2. Import the data from GCS to Cloud SQL

    ```bash
    python bq_to_sql.py gcs-to-cloudsql \
        --table-name user_statistics \
        --bucket-path gs://data-bucket-dev/export/bigquery_to_cloudsql_recommendation_tables/20240414_100000/ \
        --date 20240414
    ```

3. Refresh the materialized view in Cloud SQL

    ```bash
    python bq_to_sql.py materialize-cloudsql \
        --view-name user_statistics
    ```
