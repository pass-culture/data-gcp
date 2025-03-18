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
