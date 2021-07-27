import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.tag_offers import tag_offers
from dependencies.config import GCP_PROJECT

default_dag_args = {
    "start_date": datetime.datetime(2021, 7, 27),
    "retries": 1,
    "project_id": GCP_PROJECT,
    }

dag = DAG(
    "extract_offer_tags",
    default_args=default_dag_args,
    description="Tag offer based on description topic",
    on_failure_callback=None,
    schedule_interval="0 23 * * *",
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
