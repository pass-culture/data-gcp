import datetime
import os

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow.operators.dummy_operator import DummyOperator

from dependencies.access_gcp_secrets import access_secret_data
from dependencies.slack_alert import task_fail_slack_alert

yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(
    "%Y-%m-%d"
) + " 00:00:00"
TABLES = {
    "ab_testing_20201207": {"query": None, "write_disposition": "WRITE_TRUNCATE"},
    "past_recommended_offers": {
        "query": f"SELECT * FROM public.past_recommended_offers where date <= '{yesterday}'",
        "write_disposition": "WRITE_APPEND",
    },
}
GCP_PROJECT = os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("REGION")

RECOMMENDATION_SQL_INSTANCE = os.environ.get("RECOMMENDATION_SQL_INSTANCE")
CONNECTION_ID = os.environ.get("BIGQUERY_CONNECTION_RECOMMENDATION")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET")

# Recreate proprely the connection url
database_url = access_secret_data(
    GCP_PROJECT, f"{RECOMMENDATION_SQL_INSTANCE}-database-url"
)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    database_url.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT}&location={LOCATION}&instance={RECOMMENDATION_SQL_INSTANCE}&use_proxy=True&sql_proxy_use_tcp=True"
)

default_dag_args = {
    "start_date": datetime.datetime(2021, 2, 2),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "export_cloudsql_tables_to_bigquery_v1",
    default_args=default_dag_args,
    description="Export tables from recommendation CloudSQL to BigQuery",
    schedule_interval="0 3 * * *",
    on_failure_callback=task_fail_slack_alert,
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
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.{table}",
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
