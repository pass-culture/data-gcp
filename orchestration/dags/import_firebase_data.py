import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from dependencies.config import BIGQUERY_RAW_DATASET, ENV_SHORT_NAME, GCP_PROJECT
from dependencies.env_switcher import env_switcher
from dependencies.slack_alert import task_fail_slack_alert

ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging"],
    "prod": ["app.passculture.prod"],
}

app_info_id_list = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
EXECUTION_DATE = "{{ ds_nodash }}"

default_dag_args = {
    "start_date": datetime.datetime(2021, 3, 10),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_firebase_data_v1",
    default_args=default_dag_args,
    description="Import firebase data and dispatch it to each env",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 12 * * *" if ENV_SHORT_NAME == "prod" else "30 12 * * *",
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

env_switcher = BranchPythonOperator(
    task_id="env_switcher",
    python_callable=env_switcher,
    dag=dag,
)

copy_table = BigQueryOperator(
    task_id=f"copy_table",
    sql=f"SELECT * FROM passculture-android-natif.analytics_249283927.events_"
    + EXECUTION_DATE,
    use_legacy_sql=False,
    destination_dataset_table=f"passculture-data-prod:firebase_raw_data.events_"
    + EXECUTION_DATE,
    write_disposition="WRITE_EMPTY",
    dag=dag,
)
delete_table = BigQueryTableDeleteOperator(
    task_id=f"delete_table",
    deletion_dataset_table=f"passculture-android-natif:analytics_249283927.events_"
    + EXECUTION_DATE,
    ignore_if_missing=True,
    dag=dag,
)

dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

copy_table_to_env = BigQueryOperator(
    task_id=f"copy_table_to_env",
    sql=f"""
        SELECT * FROM passculture-data-prod.firebase_raw_data.events_{EXECUTION_DATE} WHERE app_info.id IN ({", ".join([f"'{app_info_id}'" for app_info_id in app_info_id_list])})
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_RAW_DATASET}.events_"
    + EXECUTION_DATE,
    write_disposition="WRITE_EMPTY",
    trigger_rule="none_failed",
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> env_switcher
env_switcher >> dummy_task >> copy_table_to_env
env_switcher >> copy_table >> delete_table >> copy_table_to_env >> end
