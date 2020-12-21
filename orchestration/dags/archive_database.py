import os
import ast
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import (
    CloudSqlInstanceExportOperator,
)

# Variables
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "pass-culture-app-projet-test")
GCS_BUCKET = "pass-culture-data"
INSTANCE_NAME = "dump-prod-8-10-2020"
DB_NAME = "test-restore"
ARCHIVE_FOLDER = "archive_cloudsql"

# Starting DAG
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "archive_database_v1",
    default_args=default_args,
    start_date=datetime(2020, 12, 18),
    description="Export database to create archive every day",
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

start = DummyOperator(task_id="start", dag=dag)

# File path and name.
now = datetime.now()
file_name = f"{DB_NAME}/{now.year}_{now.month}_{now.day}_{DB_NAME}.sql"
export_uri = f"gs://{GCS_BUCKET}/{ARCHIVE_FOLDER}/{file_name}"

export_body = {
    "exportContext": {
        "fileType": "sql",
        "uri": export_uri,
        "sqlExportOptions": {"schemaOnly": False},
        "databases": [DB_NAME],
    }
}

sql_export_task = CloudSqlInstanceExportOperator(
    project_id=GCP_PROJECT_ID,
    body=export_body,
    instance=INSTANCE_NAME,
    task_id="export_database",
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> sql_export_task >> end
