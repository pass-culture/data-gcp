import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

from dependencies.config import BIGQUERY_RAW_DATASET, ENV_SHORT_NAME, GCP_PROJECT
from dependencies.env_switcher import env_switcher
from dependencies.slack_alert import task_fail_slack_alert

ENV_SHORT_NAME_APP_INFO_ID_MAPPING = {
    "dev": ["app.passculture.test", "app.passculture.testing"],
    "stg": ["app.passculture.staging"],
    "prod": ["app.passculture.prod"],
}

default_dag_args = {
    "start_date": datetime.datetime(2021, 3, 3),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_firebase_data_v1",
    default_args=default_dag_args,
    description="Import firebase data and dispatch it to each env",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 6 * * *" if ENV_SHORT_NAME == "prod" else "30 6 * * *",
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=90)
)

start = DummyOperator(task_id="start", dag=dag)

env_switcher = BranchPythonOperator(
    task_id="env_switcher",
    python_callable=env_switcher,
    dag=dag,
)


def copy_and_clear_firebase_tables(**kwargs):
    client = bigquery.Client()
    tables = client.list_tables("passculture-android-natif.analytics_249283927")
    for table in tables:
        copy_table = BigQueryOperator(
            task_id=f"copy_{table.table_id}",
            sql=f"SELECT * FROM {table.project}.{table.dataset_id}.{table.table_id}",
            use_legacy_sql=False,
            destination_dataset_table=f"passculture-data-prod:firebase_raw_data.{table.table_id}",
            write_disposition="WRITE_EMPTY",
            dag=dag,
        )
        copy_table.execute(context=kwargs)
        delete_table = BigQueryOperator(
            task_id=f"delete_{table.id}",
            destination_dataset_table=False,
            bql=f"DELETE FROM {table.project}.{table.dataset_id}.{table.table_id}",
            use_legacy_sql=False,
            dag=dag,
        )
        delete_table.execute(context=kwargs)
    return


def copy_env_data(**kwargs):
    app_info_id_list = ENV_SHORT_NAME_APP_INFO_ID_MAPPING[ENV_SHORT_NAME]
    client = bigquery.Client()
    firebase_raw_data_tables = client.list_tables(
        "passculture-data-prod.firebase_raw_data"
    )
    env_raw_data_table_id = [
        table.table_id
        for table in client.list_tables(f"{GCP_PROJECT}.raw_{ENV_SHORT_NAME}")
        if "events_" in table.table_id
    ]
    missing_tables = [
        table
        for table in firebase_raw_data_tables
        if table.table_id not in env_raw_data_table_id
    ]
    for table in missing_tables:
        copy_table = BigQueryOperator(
            task_id=f"copy_and_filter_{table.table_id}",
            sql=f"SELECT * FROM passculture-data-prod.firebase_raw_data.{table.table_id} WHERE app_info.id IN ({', '.join(app_info_id_list)})",
            use_legacy_sql=False,
            destination_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_RAW_DATASET}.{table.table_id}",
            write_disposition="WRITE_EMPTY",
            dag=dag,
        )
        copy_table.execute(context=kwargs)
    return


copy_firebase_data = PythonOperator(
    task_id="copy_firebase_data",
    python_callable=copy_and_clear_firebase_tables,
    op_kwargs={},
    dag=dag,
)

dummy_task = DummyOperator(task_id="dummy_task", dag=dag)

copy_to_env = PythonOperator(
    task_id="copy_to_env_firebase_data",
    python_callable=copy_env_data,
    op_kwargs={},
    dag=dag,
    trigger_rule="none_failed"
)

end = DummyOperator(task_id="end", dag=dag)

start >> env_switcher
env_switcher >> dummy_task >> copy_to_env
env_switcher >> copy_firebase_data >> copy_to_env >> end
