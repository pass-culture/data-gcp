import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.data_analytics.config import (
    GCP_PROJECT_ID,
)
from dependencies.slack_alert import task_fail_slack_alert

yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(
    "%Y-%m-%d"
) + " 00:00:00"
CONNECTION_ID = "europe-west1.cloud_SQL_pcdata-poc-csql-recommendation"
TABLES = {
    "ab_testing_20201207": {"query": None, "write_disposition": "WRITE_TRUNCATE"},
    "past_recommended_offers": {
        "query": f"SELECT * FROM public.past_recommended_offers where date <= '{yesterday}'",
        "write_disposition": "WRITE_APPEND",
    },
}
RECO_KPI_DATASET = "algo_reco_kpi_data"

default_dag_args = {
    "on_failure_callback": task_fail_slack_alert,
    "start_date": datetime.datetime(2020, 12, 15),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "export_cloudsql_tables_to_bigquery_v2",
    default_args=default_dag_args,
    description="Export tables from CloudSQL to BigQuery",
    schedule_interval="@daily",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
)

start = DummyOperator(task_id="start", dag=dag)

export_table_tasks = []
for table in TABLES:
    query = TABLES[table]["query"]
    if query is None:
        query = f"SELECT * FROM public.{table}"
    task = BigQueryOperator(
        task_id=f"import_{table}",
        sql=f'SELECT * FROM EXTERNAL_QUERY("{CONNECTION_ID}", "{query}");',
        write_disposition=TABLES[table]["write_disposition"],
        use_legacy_sql=False,
        destination_dataset_table=f"{RECO_KPI_DATASET}.{table}",
        dag=dag,
    )
    export_table_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)

start >> export_table_tasks >> end
