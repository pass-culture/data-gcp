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
from dependencies.monitoring import get_request_click_through_reco_module

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


def get_last_data_timestamp(project_name, dataset, table, ti, **kwargs):
    bigquery_query = f"SELECT max(event_timestamp) FROM {project_name}.{dataset}.{table};"
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(bigquery_query)
    result = int(results.values[0][0].to_pydatetime().timestamp())
    print(f"result : {result} is type : {type(result)}")
    ti.xcom_push(key="from_time", value=result)


def compute_click_through_reco_module(ti, **kwargs):
    start_date = ti.xcom_pull(key="from_time")
    for group_id in groups:
        bigquery_query = get_request_click_through_reco_module(start_date, group_id)
        bigquery_client = BigQueryClient()
        result = int(bigquery_client.query(bigquery_query).values[0][0])
        ti.xcom_push(key=f"COUNT_CLICK_RECO_{group_id}", value=result)


def insert_metric_bq(project_name, dataset, table, ti, **kwargs):
    bigquery_query = f"""INSERT `{project_name}.{dataset}.{table}` (compute_time, from_time, last_metric_time, metric_name, metric_value, algorithm_id, environment, group_id) 
    VALUES"""
    last_metric_time = ti.xcom_pull(key="from_time")
    for metric_id, _ in metrics_to_compute.items():
        for group_id in groups:
            metric_query = f"""
            (   '{datetime.now()}', 
                '{START_DATE}', 
                TIMESTAMP_SECONDS(CAST(CAST({last_metric_time} as INT64)/1000000 as INT64)), 
                '{metric_id}', 
                {float(ti.xcom_pull(key=f"{metric_id}_{group_id}"))},
                'algo_v0',
                '{ENV_SHORT_NAME}',
                '{group_id}'
            )"""
            bigquery_query += metric_query
            bigquery_query += ","
    bigquery_client = BigQueryClient()
    bigquery_query = f"{bigquery_query[:-1]};"
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
        op_kwargs={
            "project_name": GCP_PROJECT,
            "dataset": BIGQUERY_ANALYTICS_DATASET,
            "table": FIREBASE_EVENTS_TABLE,
        },
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
        op_kwargs={
            "project_name": GCP_PROJECT,
            "dataset": BIGQUERY_ANALYTICS_DATASET,
            "table": MONITORING_TABLE,
        },
        provide_context=True,
    )

    end = DummyOperator(task_id="end")


start >> init_dag >> compute_metric_task >> insert_metric_bq >> end
