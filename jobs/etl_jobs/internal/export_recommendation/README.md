# Export Recommendation Job

This job exports data from BigQuery tables to a Cloud SQL database for recommendation purposes.

## Job Logic

1. **Data Export from BigQuery**
   - For each table defined in the configuration:
     - Filter columns from BigQuery source tables
     - Export filtered data to GCS as CSV files
     - Compose multiple CSV files into a single file if needed

2. **Cloud SQL Import**
   - For each table:
     - Drop existing table if exists
     - Create new table with proper schema
     - Import data from GCS CSV files

3. **Materialized Views**
   - After data import, refresh materialized views in the following order:
     - Concurrent views:
       - enriched_user_mv
       - item_ids_mv
       - non_recommendable_items_mv
       - iris_france_mv
     - Sequential view:
       - recommendable_offers_raw_mv

## Usage

```bash
# Run the job
python main.py export-recommendation
```

## Configuration

The job uses the following environment variables:
- ENV_SHORT_NAME: Environment short name (e.g., 'prod', 'staging')
- GCP_PROJECT_ID: GCP project ID
- BIGQUERY_ML_RECOMMENDATION_DATASET: BigQuery ML recommendation dataset
- BIGQUERY_SEED_DATASET: BigQuery seed dataset
- RECOMMENDATION_SQL_INSTANCE: Cloud SQL instance name

## Tables

The job handles the following tables:
1. enriched_user
2. recommendable_offers_raw
3. non_recommendable_items_data
4. iris_france

Each table has its own schema defined in the configuration.
