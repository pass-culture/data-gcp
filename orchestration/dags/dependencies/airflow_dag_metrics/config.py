from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID
from google.cloud import bigquery

DESTINATION_DATASET = f"raw_{ENV_SHORT_NAME}"
DESTINATION_TABLE = "airflow_dag_metrics"
FULL_TABLE_ID = f"{GCP_PROJECT_ID}.{DESTINATION_DATASET}.{DESTINATION_TABLE}"

BQ_SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE"),
    bigquery.SchemaField("environment", "STRING"),
    bigquery.SchemaField("dag_id", "STRING"),
    bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
    bigquery.SchemaField("schedule_interval", "STRING"),
    bigquery.SchemaField("dag_run_state", "STRING"),
    bigquery.SchemaField("dag_run_execution_date", "TIMESTAMP"),
    bigquery.SchemaField("tasks_success", "INTEGER"),
    bigquery.SchemaField("tasks_failed", "INTEGER"),
    bigquery.SchemaField("tasks_skipped", "INTEGER"),
    bigquery.SchemaField("tasks_upstream_failed", "INTEGER"),
]
