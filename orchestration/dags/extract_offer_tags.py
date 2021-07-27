import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.tag_offers import tag_offers
from dependencies.config import GCP_PROJECT

default_dag_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "extracted_data_tag_offers",
    default_args=default_dag_args,
    description="Tag offer based on description topic",
    on_failure_callback=None,
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)


tag_offers = PythonOperator(
    task_id=f"tag_offers",
    python_callable=tag_offers,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

(start >> tag_offers >> end)
