import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.top_common_ngrams import save_top_common
from dependencies.config import GCP_PROJECT, DATA_GCS_BUCKET_NAME


default_dag_args = {
    "start_date": datetime.datetime(2021, 7, 30),
    "retries": 1,
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "extract_top_common_ngram",
    default_args=default_dag_args,
    description="Extract the top ngram",
    on_failure_callback=None,
    schedule_interval="0 0 1 * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)


top_common_ngram = PythonOperator(
    task_id=f"top_common_ngram",
    python_callable=save_top_common,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

(start >> top_common_ngram >> end)
