import os
import ast
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import (
    CloudSqlInstanceExportOperator,
)
from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    APPLICATIVE_SQL_INSTANCE,
    APPLICATIVE_SQL_DATABASE,
)

# Variables
ARCHIVE_FOLDER = "archive_cloudsql"

# Starting DAG
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "archive_database_v2",
    default_args=default_args,
    start_date=datetime(2020, 12, 18),
    description="Export database to create archive every day",
    schedule_interval="0 22 * * *",
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

start = DummyOperator(task_id="start", dag=dag)

now = datetime.now()
file_name = f"{APPLICATIVE_SQL_DATABASE}/{now.year}_{now.month}_{now.day}_{APPLICATIVE_SQL_DATABASE}.sql"
export_uri = f"gs://{DATA_GCS_BUCKET_NAME}/{ARCHIVE_FOLDER}/{file_name}"

export_body = {
    "exportContext": {
        "fileType": "sql",
        "uri": export_uri,
        "sqlExportOptions": {"schemaOnly": False},
        "databases": [APPLICATIVE_SQL_DATABASE],
    }
}

sql_export_task = CloudSqlInstanceExportOperator(
    project_id=GCP_PROJECT,
    body=export_body,
    instance=APPLICATIVE_SQL_INSTANCE,
    task_id="export_database",
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> sql_export_task >> end
