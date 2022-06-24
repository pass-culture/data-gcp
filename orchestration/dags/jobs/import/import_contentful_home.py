import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from common.alerts import task_fail_slack_alert
from dependencies.import_contentful_home import (
    load_from_csv_contentful_file,
    create_table_contentful_home,
)
from common.config import (
    GCP_PROJECT,
)

default_dag_args = {
    "start_date": datetime.datetime(2021, 3, 7),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "create_contentful_home",
    default_args=default_dag_args,
    description="Import from storage contentful home",
    schedule_interval="0 * * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

create_contentful_home_table = BigQueryExecuteQueryOperator(
    task_id="create_contentful_home_table",
    sql=create_table_contentful_home(),
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

create_contentful_home = PythonOperator(
    task_id="create_contentful_home",
    python_callable=load_from_csv_contentful_file,
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)


(start >> create_contentful_home_table >> create_contentful_home >> end)
