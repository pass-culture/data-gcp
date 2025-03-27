# Export Recommendation Job

This job exports data from BigQuery tables to a Cloud SQL database for recommendation purposes.

## Job Logic

1. **Data Export from BigQuery**
   - For each table defined in the configuration:
     - Filter columns from BigQuery source tables
     - Export filtered data to GCS as Parquet files with Snappy compression
     - Parquet format provides better performance and type safety for large datasets

2. **Cloud SQL Import**
   - For each table:
     - Download Parquet files from GCS
     - Use DuckDB as an intermediary for efficient data transfer
     - Drop existing table if exists
     - Create new table with proper schema
     - Import data directly from Parquet to PostgreSQL using DuckDB's PostgreSQL extension
     - Clean up temporary files

3. **Materialized Views**
   - After data import, refresh materialized views:
     - SQL templates are rendered using Jinja2, replacing variables like `{{ ts_nodash }}` with actual values
     - All views are refreshed concurrently:
       - enriched_user_mv
       - item_ids_mv
       - non_recommendable_items_mv
       - iris_france_mv
       - recommendable_offers_raw_mv

## Usage

The job now uses Typer for command-line interface, providing separate commands for each operation:

```bash
# Export a table to GCS
python main.py export --table-name <table_name> --bucket-path <bucket_path> --date <YYYYMMDD>

# Import a table from GCS to Cloud SQL
python main.py import-table --table-name <table_name> --bucket-path <bucket_path> --date <YYYYMMDD>

# Refresh a materialized view
python main.py materialize --view-name <view_name>
```

### Parameters

- `--table-name`: Name of the table to process (must be defined in configuration)
- `--bucket-path`: Full GCS path for temporary storage (e.g., gs://bucket-name/recommendation_exports)
- `--date`: Date in YYYYMMDD format (used for file naming and versioning)
- `--view-name`: Name of the materialized view to refresh (for materialize command only)

## Configuration

The job uses the following environment variables:
- `ENV_SHORT_NAME`: Environment short name (e.g., 'prod', 'staging')
- `GCP_PROJECT_ID`: GCP project ID
- `BIGQUERY_ML_RECOMMENDATION_DATASET`: BigQuery ML recommendation dataset
- `BIGQUERY_SEED_DATASET`: BigQuery seed dataset
- `RECOMMENDATION_SQL_INSTANCE`: Cloud SQL instance name

## Database Connection

The job connects directly to the PostgreSQL database using the database URL stored in Secret Manager:
- `{RECOMMENDATION_SQL_INSTANCE}_database_url`: PostgreSQL connection string in the format `postgresql://<user>:<pass>@<uri>/<db>`

## Tables

The job handles the following tables:
1. enriched_user
2. recommendable_offers_raw
3. non_recommendable_items_data
4. iris_france

Each table has its own schema defined in the configuration.

## SQL Templates

The SQL files for materialized views use Jinja2 templating:
- Variables like `{{ ts_nodash }}` are replaced with actual values at runtime
- This allows for dynamic SQL generation with timestamps for unique function and index names
- The templates are stored in the `sql/` directory

## Temporary Tables

Temporary tables are created in the `tmp_{ENV_SHORT_NAME}` dataset during export operations to ensure proper isolation of temporary resources.

## Performance Considerations

- The job uses Parquet format with Snappy compression for efficient storage and transfer
- DuckDB provides high-performance data processing capabilities
- DuckDB's PostgreSQL extension allows direct data transfer between Parquet files and PostgreSQL
- This approach eliminates the need for intermediate data transformations and type handling
- Highly efficient for processing millions of rows

# Data Export/Import Management

This tool manages the bidirectional data flow between BigQuery and CloudSQL for recommendation data:

1. **BigQuery to CloudSQL**: Exports tables from BigQuery, uploads to GCS, and imports into CloudSQL
2. **CloudSQL to BigQuery**: Exports tables from CloudSQL to BigQuery using hourly incremental exports

## Prerequisites

- Python 3.12+
- Google Cloud SDK installed and configured
- Access to relevant Google Cloud resources (BQ, GCS, CloudSQL)
- Required dependencies installed (`pip install -r requirements.txt`)

## BigQuery to CloudSQL Export

### Usage

```bash
# Export from BigQuery to GCS
python main.py export-gcs \
  --table-name <table_name> \
  --bucket-path gs://<bucket-name>/path \
  --date <YYYYMMDD>

# Import from GCS to CloudSQL
python main.py import-to-gcloud \
  --table-name <table_name> \
  --bucket-path gs://<bucket-name>/path \
  --date <YYYYMMDD>

# Refresh a materialized view in CloudSQL
python main.py materialize-gcloud \
  --view-name <view_name>
```

## CloudSQL to BigQuery Hourly Export

### Overview

The hourly export functionality allows exporting data incrementally from CloudSQL to BigQuery at hourly intervals. This is particularly useful for high-volume tables like `past_offer_context` that need more frequent updates.

### Usage

```bash
# Full hourly export process (CloudSQL -> GCS -> BigQuery)
python main.py hourly_export_process \
  --table-name past_offer_context \
  --bucket-path gs://<bucket-name>/export/cloudsql_hourly \
  --date <YYYYMMDD> \
  --hour <0-23> \
  [--delete-from-source] \
  [--recover-missed]

# Individual steps can also be run separately:


# 2. Load data from GCS to BigQuery only
python main.py hourly_load_gcs_to_bigquery \
  --table-name past_offer_context \
  --bucket-path gs://<bucket-name>/export/cloudsql_hourly \
  --date <YYYYMMDD> \
  --hour <0-23>
```

### Recovery Mode

The tool has a built-in recovery mechanism to handle failures:

- When `--recover-missed` is enabled (default in the Airflow DAG), the tool will:
  1. Check for any gaps in the export history by examining GCS
  2. Automatically export any missing data since the last successful export
  3. Process each missing hour in chronological order
  4. Add all data to the appropriate daily partitions in BigQuery

This ensures that no data is lost even if some hourly jobs fail, making the system resilient to temporary outages or infrastructure issues.

### Notes on Hourly Exports

- Data is exported based on the `date` column and hour range
- Exports only include data for the specified hour
- Data is written to daily partitions in BigQuery, with an additional `import_hour` column
- The `--delete-from-source` flag will remove processed data from CloudSQL after successful export, helping to prevent duplicate data and reduce database size

## Airflow Integration

The exports are scheduled and orchestrated using Airflow DAGs:

- `sync_bq_to_cloudsql_recommendation_tables` - Daily export from BigQuery to CloudSQL
- `export_cloudsql_tables_to_bigquery_hourly` - Hourly export from CloudSQL to BigQuery

## Architecture

The tool uses DuckDB as an intermediate layer for efficient data processing and transformation, enabling:

1. High-performance data exports and imports
2. Memory-efficient processing of large datasets
3. Direct interaction with GCS and databases without excessive memory usage
