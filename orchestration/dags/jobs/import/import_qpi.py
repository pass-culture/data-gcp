from datetime import datetime, timedelta
import time
from google.cloud import storage
from common.config import DAG_FOLDER
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    GCP_PROJECT_ID,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
)
from common.operators.biquery import bigquery_job_task
from dependencies.qpi.import_qpi import (
    QPI_ANSWERS_SCHEMA,
    RAW_TABLES,
    CLEAN_TABLES,
    ANALYTICS_TABLES,
)
from common.utils import depends_loop

from common.utils import get_airflow_schedule

TYPEFORM_FUNCTION_NAME = "qpi_import_" + ENV_SHORT_NAME
QPI_ANSWERS_TABLE = "qpi_answers_v4"

default_args = {
    "start_date": datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def verify_folder():
    today = time.strftime("%Y%m%d")
    storage_client = storage.Client()
    bucket = storage_client.bucket(DATA_GCS_BUCKET_NAME)
    name = f"QPI_exports/qpi_answers_{today}/"
    stats = bucket.list_blobs(prefix=name)
    blob_list = []
    for s in stats:
        blob_list.append(s)
    if len(blob_list) > 0:
        return "Files"
    else:
        return "Empty"


with DAG(
    "import_qpi_answers_v1",
    default_args=default_args,
    description="Importing new data from QPI every day.",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
) as dag:

    start = DummyOperator(task_id="start")

    checking_folder_QPI = BranchPythonOperator(
        task_id="checking_folder_QPI", python_callable=verify_folder
    )
    file = DummyOperator(task_id="Files")
    empty = DummyOperator(task_id="Empty")

    # it fetches the file corresponding to the initial execution date of the dag and not the day the task is run.
    import_answers_to_bigquery = GCSToBigQueryOperator(
        task_id="import_answers_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=["QPI_exports/qpi_answers_{{ ds_nodash }}/*.jsonl"],
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=False,
        schema_fields=QPI_ANSWERS_SCHEMA,
    )

    raw_tasks = []
    for table, params in RAW_TABLES.items():
        task = bigquery_job_task(
            dag=dag, table=f"add_{table}_to_raw", job_params=params
        )
        raw_tasks.append(task)

    end_raw = DummyOperator(task_id="end_raw")

    clean_tasks = []
    for table, params in CLEAN_TABLES.items():
        if params.get('load_table', True) is True:
            task = bigquery_job_task(
                dag=dag, table=f"add_{table}_to_clean", job_params=params
            )
            clean_tasks.append(task)

    end_clean = DummyOperator(task_id="end_clean")

    delete_temp_answer_table_raw = BigQueryDeleteTableOperator(
        task_id="delete_temp_answer_table_raw",
        deletion_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}",
        ignore_if_missing=True,
        dag=dag,
        trigger_rule="none_failed_or_skipped",
    )

    start_analytics = DummyOperator(task_id="start_analytics")
    analytics_tables_jobs = {}
    for table, job_params in ANALYTICS_TABLES.items():
        task = bigquery_job_task(dag=dag, table=table, job_params=job_params)
        analytics_tables_jobs[table] = {
            "operator": task,
            "depends": job_params.get("depends", []),
            "dag_depends": job_params.get("dag_depends", []),
        }

    analytics_tables_jobs = depends_loop(
        analytics_tables_jobs, start_analytics, dag=dag
    )

    end = DummyOperator(task_id="end", trigger_rule="one_success")

    (
        start
        >> checking_folder_QPI
        >> file
        >> import_answers_to_bigquery
        >> raw_tasks
        >> end_raw
        >> clean_tasks
        >> end_clean
        >> delete_temp_answer_table_raw
        >> start_analytics
        >> analytics_tables_jobs
        >> end
    )
    (checking_folder_QPI >> empty >> end)
