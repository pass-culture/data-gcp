import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.data_analytics.config import (
    GCP_PROJECT_ID,
)
from dependencies.slack_alert import task_fail_slack_alert

CONNECTION_ID = "europe-west1.cloud_SQL_pcdata-poc-csql-recommendation"
AB_TESTING_TABLES = ["ab_testing_20201207"]
BIGQUERY_AB_TESTING_DATASET = "algo_reco_kpi_data"

default_dag_args = {
    "on_failure_callback": task_fail_slack_alert,
    "start_date": datetime.datetime(2020, 12, 7),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "export_ab_testing_tables_v4",
    default_args=default_dag_args,
    description="Export A/B testing tables from CloudSQL to BigQuery",
    schedule_interval="@daily",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

export_table_tasks = []
for table in AB_TESTING_TABLES:
    task = BigQueryOperator(
        task_id=f"import_{table}",
        sql=f"SELECT * FROM EXTERNAL_QUERY('{CONNECTION_ID}', 'SELECT * FROM public.{table}');",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_AB_TESTING_DATASET}.{table}",
        dag=dag,
    )
    export_table_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)

start >> export_table_tasks >> end
