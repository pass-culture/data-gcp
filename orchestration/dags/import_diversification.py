import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.diversification_data import (
    main_get_data,
)
from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
)

default_dag_args = {
    "start_date": datetime.datetime(2022, 3, 30),
    "retries": 1,
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "create_diversification_KPI",
    default_args=default_dag_args,
    description="Import data and calculate diversification",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 * * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

import_data_diversification = PythonOperator(
    task_id="import_data_diversification",
    python_callable=main_get_data,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.user_diversification",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)


(start >> import_data_diversification >> end)
