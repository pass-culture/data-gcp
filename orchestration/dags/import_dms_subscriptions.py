import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import (
    GoogleCloudStorageToBigQueryOperator,
)

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.request_dms import update_dms_applications

from dependencies.config import DATA_GCS_BUCKET_NAME, BIGQUERY_ANALYTICS_DATASET


default_args = {
    "start_date": datetime(2021, 5, 26),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "import_dms_subscriptions",
    default_args=default_args,
    description="Import DMS subscriptions",
    schedule_interval="0 1 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    fetch_dms = PythonOperator(
        task_id=f"download_dms_subscriptions",
        python_callable=update_dms_applications,
    )

    now = datetime.now()
    import_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="import_dms_to_bq",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[f"dms_export/dms_{now.year}_{now.month}_{now.day}.csv"],
        destination_project_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.dms_applications_api",
        schema_fields=[
            {"name": "procedure_id", "type": "STRING"},
            {"name": "application_id", "type": "STRING"},
            {"name": "application_status", "type": "STRING"},
            {"name": "last_update_at", "type": "TIMESTAMP"},
            {"name": "application_submitted_at", "type": "TIMESTAMP"},
            {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
            {"name": "processed_at", "type": "TIMESTAMP"},
            {"name": "instructor_mail", "type": "STRING"},
            {"name": "applicant_department", "type": "STRING"},
            {"name": "applicant_birthday", "type": "STRING"},
            {"name": "applicant_postal_code", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    end = DummyOperator(task_id="end")


start >> fetch_dms >> import_to_bq >> end
