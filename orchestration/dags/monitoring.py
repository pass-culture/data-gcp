import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.gcs_to_bq import (
    GoogleCloudStorageToBigQueryOperator,
)

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.bigquery_client import BigQueryClient
from dependencies.monitoring import (
    get_request_click_through_reco_module,
    get_insert_metric_request,
    get_last_event_time_request,
)

from dependencies.config import (
    GCP_PROJECT,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)

FIREBASE_EVENTS_TABLE = "firebase_events"
MONITORING_TABLE = "monitoring_data"
START_DATE = datetime(2021, 5, 20)
groups = ["A", "B"]

LAST_EVENT_TIME_KEY = "last_event_time_key"


def get_last_data_timestamp(ti, **kwargs):
    bigquery_client = BigQueryClient()
    bigquery_query = get_last_event_time_request()
    results = bigquery_client.query(bigquery_query)
    result = int(results.values[0][0].to_pydatetime().timestamp())
    ti.xcom_push(key=LAST_EVENT_TIME_KEY, value=result)


def compute_click_through_reco_module(ti, **kwargs):
    start_date = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    for group_id in groups:
        bigquery_query = get_request_click_through_reco_module(start_date, group_id)
        bigquery_client = BigQueryClient()
        result = int(bigquery_client.query(bigquery_query).values[0][0])
        ti.xcom_push(key=f"COUNT_CLICK_RECO_{group_id}", value=result)


def insert_metric_bq(ti, **kwargs):
    bigquery_client = BigQueryClient()
    bigquery_query = get_insert_metric_request(ti, START_DATE)
    bigquery_client.query(bigquery_query)


metrics_to_compute = {
    "COUNT_CLICK_RECO": compute_click_through_reco_module,
}


default_args = {
    "start_date": datetime(2021, 5, 26),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "compute_monitoring",
    default_args=default_args,
    description="Compute monitoring metrics",
    schedule_interval="0 1 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    init_dag = PythonOperator(
        task_id="get_last_data_timestamp",
        python_callable=get_last_data_timestamp,
        provide_context=True,
    )

    compute_metric_task = [
        PythonOperator(
            task_id=f"compute_{metric}",
            python_callable=function_to_call,
            provide_context=True,
        )
        for metric, function_to_call in metrics_to_compute.items()
    ]

    insert_metric_bq = PythonOperator(
        task_id=f"insert_metric_bigquery",
        python_callable=insert_metric_bq,
        provide_context=True,
    )

    end = DummyOperator(task_id="end")


start >> init_dag >> compute_metric_task >> insert_metric_bq >> end
