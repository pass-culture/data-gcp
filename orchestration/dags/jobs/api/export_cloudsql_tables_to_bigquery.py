import datetime
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator

from dependencies.export_cloudsql_tables_to_bigquery.import_cloudsql import (
    RAW_TABLES,
    CLEAN_TABLES,
    ANALYTICS_TABLES,
)
from common.access_gcp_secrets import access_secret_data
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_CLEAN_DATASET,
)
from common.utils import from_external
from common import macros

yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(
    "%Y-%m-%d"
) + " 00:00:00"

GCP_PROJECT = os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("REGION")
DAG_FOLDER = os.environ.get("DAG_FOLDER")

RECOMMENDATION_SQL_INSTANCE = os.environ.get("RECOMMENDATION_SQL_INSTANCE")
RECOMMENDATION_SQL_BASE = os.environ.get("RECOMMENDATION_SQL_BASE")
CONNECTION_ID = os.environ.get("BIGQUERY_CONNECTION_RECOMMENDATION")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET")

# Recreate proprely the connection url
database_url = access_secret_data(
    GCP_PROJECT, f"{RECOMMENDATION_SQL_BASE}-database-url", default=""
)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    database_url.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT}&location={LOCATION}&instance={RECOMMENDATION_SQL_INSTANCE}&use_proxy=True&sql_proxy_use_tcp=True"
)

default_dag_args = {
    "start_date": datetime.datetime(2021, 2, 2),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT,
}

dag = DAG(
    "export_cloudsql_tables_to_bigquery_v1",
    default_args=default_dag_args,
    description="Export tables from recommendation CloudSQL to BigQuery",
    schedule_interval="0 3 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

export_raw_table_tasks = []
for table, params in RAW_TABLES.items():
    query = params["sql"]
    if query is None:
        query = f"SELECT * FROM public.{table}"
    task = BigQueryInsertJobOperator(
        task_id=table,
        configuration={
            "query": {
                "query": from_external(conn_id=CONNECTION_ID, sql_path=params["sql"]),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BIGQUERY_RAW_DATASET,
                    "tableId": table,
                },
                "writeDisposition": params["write_disposition"],
            }
        },
        params=dict(params.get("params", {})),
        dag=dag,
    )

    export_raw_table_tasks.append(task)

delete_rows_task = drop_table_task = CloudSQLExecuteQueryOperator(
    task_id="drop_yesterday_rows_past_recommended_offers",
    gcp_cloudsql_conn_id="proxy_postgres_tcp",
    sql=f"DELETE FROM public.past_recommended_offers where date <= '{yesterday}'",
    autocommit=True,
    dag=dag,
)

export_clean_table_tasks = []
for table, params in CLEAN_TABLES.items():
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_{table}_in_clean",
        sql=params["sql"],
        write_disposition=params["write_disposition"],
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.{table}",
        time_partitioning=params["time_partitioning"],
        cluster_fields=params.get("cluster_fields", None),
        dag=dag,
    )
    export_clean_table_tasks.append(task)

end_clean = DummyOperator(task_id="end_clean", dag=dag)

export_analytics_table_tasks = []
for table, params in ANALYTICS_TABLES.items():
    task = BigQueryExecuteQueryOperator(
        task_id=f"import_{table}_in_analytics",
        sql=params["sql"],
        write_disposition=params["write_disposition"],
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.{table}",
        time_partitioning=params["time_partitioning"],
        cluster_fields=params.get("cluster_fields", None),
        dag=dag,
    )
    export_analytics_table_tasks.append(task)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> export_raw_table_tasks
    >> delete_rows_task
    >> export_clean_table_tasks
    >> end_clean
    >> export_analytics_table_tasks
    >> end
)
(delete_rows_task >> end)
