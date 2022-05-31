import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator

from dependencies.config import (
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    GCP_PROJECT,
)

from common.alerts import task_fail_slack_alert

EXECUTION_DATE = "{{ ds_nodash }}"

default_dag_args = {
    "start_date": datetime.datetime(2022, 5, 30),
    "retries": 3,
    "retry_delay": datetime.timedelta(hours=6),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "import_offer_bookability_log",
    default_args=default_dag_args,
    description="Import active offer log",
    on_failure_callback=task_fail_slack_alert,
    schedule_interval="0 0 * * *",
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

import_offer_bookability_log = BigQueryExecuteQueryOperator(
    task_id="import_to_analytics_offer_bookability_log",
    sql=f"""
        SELECT CURRENT_DATE() AS date, offer_id FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer WHERE offer_is_active=TRUE 
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.offer_bookability_log_{EXECUTION_DATE}",
    write_disposition="WRITE_EMPTY",
    trigger_rule="none_failed",
    dag=dag,
)

import_collective_offer_bookability_log = BigQueryExecuteQueryOperator(
    task_id="import_to_analytics_collective_offer_bookability_log",
    sql=f"""
        SELECT CURRENT_DATE() AS date, collective_offer_id FROM {GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_collective_offer WHERE collective_offer_is_active=TRUE 
        """,
    use_legacy_sql=False,
    destination_dataset_table=f"{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.collective_offer_bookability_log_{EXECUTION_DATE}",
    write_disposition="WRITE_EMPTY",
    trigger_rule="none_failed",
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

start >> [import_offer_bookability_log, import_collective_offer_bookability_log] >> end
