import datetime
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator

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
RECOMMENDATION_SQL_USER = os.environ.get("RECOMMENDATION_SQL_USER")
RECOMMENDATION_SQL_PASSWORD = os.environ.get("RECOMMENDATION_SQL_PASSWORD")
RECOMMENDATION_SQL_PUBLIC_IP = os.environ.get("RECOMMENDATION_SQL_PUBLIC_IP")
RECOMMENDATION_SQL_PUBLIC_PORT = os.environ.get("RECOMMENDATION_SQL_PUBLIC_PORT")
RECOMMENDATION_SQL_INSTANCE = "pcdata-poc-csql-recommendation"
RECOMMENDATION_SQL_BASE = "pcdata-poc-csql-recommendation"
TYPE = "postgres"
LOCATION = "europe-west1"

os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    f"gcpcloudsql://{RECOMMENDATION_SQL_USER}:{RECOMMENDATION_SQL_PASSWORD}@{RECOMMENDATION_SQL_PUBLIC_IP}:{RECOMMENDATION_SQL_PUBLIC_PORT}/{RECOMMENDATION_SQL_BASE}?"
    f"database_type={TYPE}&"
    f"project_id={GCP_PROJECT_ID}&"
    f"location={LOCATION}&"
    f"instance={RECOMMENDATION_SQL_INSTANCE}&"
    f"use_proxy=True&"
    f"sql_proxy_use_tcp=True"
)

default_dag_args = {
    "on_failure_callback": task_fail_slack_alert,
    "start_date": datetime.datetime(2020, 12, 16),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "export_cloudsql_tables_to_bigquery_v3",
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

delete_rows_task = drop_table_task = CloudSqlQueryOperator(
    task_id=f"drop_yesterday_rows_past_recommended_offers",
    gcp_cloudsql_conn_id="proxy_postgres_tcp",
    sql=f"DELETE FROM public.past_recommended_offers where date <= '{yesterday}'",
    autocommit=True,
    dag=dag,
)


end = DummyOperator(task_id="end", dag=dag)

start >> export_table_tasks >> delete_rows_task >> end
