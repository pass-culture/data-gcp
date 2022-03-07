import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from dependencies.import_contentful_file import (
    load_from_csv_contentful_file,
)
from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
)

default_dag_args = {
    "start_date": datetime.datetime(2021, 3, 7),
    "retries": 1,
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_contentful_home",
    default_args=default_dag_args,
    description="Import from storage contentful home",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 * * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)


create_contentful_home = BigQueryOperator(
    task_id="create_contentful_home",
    python_callable=load_from_csv_contentful_file(),
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.contentful_home",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)


(start >> create_contentful_home >> end)
