import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.tag_offers import (
    tag_offers_description,
    tag_offers_name,
    get_offers_to_tag,
    update_table,
)
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

get_offers_to_tag = PythonOperator(
    task_id=f"get_offers_to_tag",
    python_callable=get_offers_to_tag,
    dag=dag,
)

tag_offers_description = PythonOperator(
    task_id=f"tag_offers_description",
    python_callable=tag_offers_description,
    dag=dag,
)

tag_offers_name = PythonOperator(
    task_id=f"tag_offers_name",
    python_callable=tag_offers_name,
    dag=dag,
)

update_table = PythonOperator(
    task_id=f"update_table",
    python_callable=update_table,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

tag_offers = [
    tag_offers_description,
    tag_offers_name,
]

(start >> get_offers_to_tag >> tag_offers >> update_table >> end)
