import datetime
import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from common.alerts import task_fail_slack_alert
from dependencies.tag_offers import (
    tag_offers_description,
    tag_offers_name,
    get_offers_to_tag_request,
    prepare_table,
    get_upsert_request,
    FILENAME_INITIAL,
)

from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_CLEAN_DATASET,
)

default_dag_args = {
    "start_date": datetime.datetime(2021, 7, 27),
    "retries": 1,
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "extract_offer_tags",
    default_args=default_dag_args,
    description="Tag offer based on description topic",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 * * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

start = DummyOperator(task_id="start", dag=dag)

get_offers_to_tag = BigQueryOperator(
    task_id="get_offers_to_tag",
    sql=get_offers_to_tag_request(),
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.temp_offers_to_tag",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

tag_offers_description = PythonOperator(
    task_id="tag_offers_description",
    python_callable=tag_offers_description,
    dag=dag,
)

tag_offers_name = PythonOperator(
    task_id="tag_offers_name",
    python_callable=tag_offers_name,
    dag=dag,
)

prepare_table = PythonOperator(
    task_id="prepare_temp_offer_tagged_table",
    python_callable=prepare_table,
    dag=dag,
)

update_offer_tags_table = BigQueryOperator(
    task_id="update_offer_tags_table",
    sql=get_upsert_request(),
    use_legacy_sql=False,
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

tag_offers = [
    tag_offers_description,
    tag_offers_name,
]

(
    start
    >> get_offers_to_tag
    >> tag_offers
    >> prepare_table
    >> update_offer_tags_table
    >> end
)
