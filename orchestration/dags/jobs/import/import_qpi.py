import json
from datetime import datetime, timedelta, date
import pandas as pd
import time
import os
from google.cloud import storage

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from common.alerts import task_fail_slack_alert
from dependencies.import_qpi import QPI_ANSWERS_SCHEMA, UNION_ALL_SQL, enrich_answers
from common.config import (
    GCP_PROJECT_ID,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)

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

    add_answers_to_raw = BigQueryExecuteQueryOperator(
        task_id="add_answers_to_raw",
        sql=f"""
            select *
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}` 
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_RAW_DATASET}.{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_APPEND",
        trigger_rule="none_failed_or_skipped",
    )

    add_answers_to_clean = BigQueryExecuteQueryOperator(
        task_id="add_answers_to_clean",
        sql=f"""
            select raw_answers.user_id,
            submitted_at, answers,
            CAST(NULL AS STRING) AS catch_up_user_id
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}` raw_answers
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_CLEAN_DATASET}.{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_APPEND",
        trigger_rule="none_failed_or_skipped",
    )

    add_temp_answers_to_clean = BigQueryExecuteQueryOperator(
        task_id="add_temp_answers_to_clean",
        sql=f"""
            select  raw_answers.user_id,
            submitted_at, answers,
            CAST(NULL AS STRING) AS catch_up_user_id
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}` raw_answers
        """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_CLEAN_DATASET}.temp_{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        trigger_rule="none_failed_or_skipped",
    )

    delete_temp_answer_table_raw = BigQueryDeleteTableOperator(
        task_id="delete_temp_answer_table_raw",
        deletion_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.temp_{QPI_ANSWERS_TABLE}",
        ignore_if_missing=True,
        dag=dag,
        trigger_rule="none_failed_or_skipped",
    )

    enrich_qpi_answers = BigQueryExecuteQueryOperator(
        task_id="enrich_qpi_answers",
        sql=enrich_answers(
            gcp_project=GCP_PROJECT_ID,
            bigquery_clean_dataset=BIGQUERY_CLEAN_DATASET,
            qpi_table=QPI_ANSWERS_TABLE,
        ),
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_ANALYTICS_DATASET}.enriched_{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        trigger_rule="none_failed_or_skipped",
    )

    clean_answers_duplicates = BigQueryExecuteQueryOperator(
        task_id="clean_answers_duplicates",
        sql=f"""
            with base as (
                SELECT *,
                ROW_NUMBER() OVER (PARTITION BY user_id,subcategories order by subcategories DESC) row_number
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.enriched_{QPI_ANSWERS_TABLE}`
            )
            select * except(row_number) from base 
            where row_number=1
            and subcategories is not null 
            and subcategories <> ""
            """,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_ANALYTICS_DATASET}.enriched_{QPI_ANSWERS_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        trigger_rule="none_failed_or_skipped",
    )

    aggregated_past_qpi_answers = BigQueryExecuteQueryOperator(
        task_id="aggregated_past_qpi_answers",
        sql=UNION_ALL_SQL,
        use_legacy_sql=False,
        destination_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_ANALYTICS_DATASET}.enriched_aggregated_qpi_answers",
        write_disposition="WRITE_TRUNCATE",
        trigger_rule="none_failed_or_skipped",
    )

    end = DummyOperator(task_id="end", trigger_rule="one_success")

    (
        start
        >> checking_folder_QPI
        >> file
        >> import_answers_to_bigquery
        >> add_answers_to_raw
        >> add_answers_to_clean
        >> add_temp_answers_to_clean
        >> delete_temp_answer_table_raw
        >> enrich_qpi_answers
        >> clean_answers_duplicates
        >> aggregated_past_qpi_answers
        >> end
    )
    (checking_folder_QPI >> empty >> end)
