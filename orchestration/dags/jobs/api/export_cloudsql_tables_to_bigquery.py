import datetime
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator

from dependencies.export_cloudsql_tables_to_bigquery.import_cloudsql import (
    TMP_TABLES,
    RAW_TABLES,
    CLEAN_TABLES,
)
from common.access_gcp_secrets import access_secret_data
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_TMP_DATASET,
    RECOMMENDATION_SQL_INSTANCE,
    CONNECTION_ID,
)
from common.utils import from_external, get_airflow_schedule
from common import macros
from common.operators.biquery import bigquery_job_task

yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(
    "%Y-%m-%d"
) + " 00:00:00"

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LOCATION = os.environ.get("REGION")
DAG_FOLDER = os.environ.get("DAG_FOLDER")

# Recreate proprely the connection url
database_instance_name = access_secret_data(
    GCP_PROJECT_ID, f"{RECOMMENDATION_SQL_INSTANCE}_database_instance_name", default=""
)

database_url = access_secret_data(
    GCP_PROJECT_ID, f"{RECOMMENDATION_SQL_INSTANCE}_database_url", default=""
)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    database_url.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT_ID}&location={LOCATION}&instance={database_instance_name}&use_proxy=True&sql_proxy_use_tcp=True"
)


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag = DAG(
    "export_cloudsql_tables_to_bigquery_v1",
    default_args=default_dag_args,
    description="Export tables from recommendation CloudSQL to BigQuery",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=90),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

start = DummyOperator(task_id="start", dag=dag)

export_tmp_table_tasks = []
for table, params in TMP_TABLES.items():
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
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": params["destination_dataset"],
                    "tableId": params["destination_table"],
                },
                "writeDisposition": params["write_disposition"],
            }
        },
        params=dict(params.get("params", {})),
        dag=dag,
    )

    export_tmp_table_tasks.append(task)

end_export_tmp_tables = DummyOperator(task_id="export_tmp_tables", dag=dag)


delete_rows = []
for delete_table_rows in [
    "past_recommended_offers",
    "past_similar_offers",
    "past_offer_context",
]:
    delete_job = drop_table_task = CloudSQLExecuteQueryOperator(
        task_id=f"drop_yesterday_rows_{delete_table_rows}",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql=f"DELETE FROM public.{delete_table_rows} where date <= '{yesterday}'",
        autocommit=True,
        dag=dag,
    )
    delete_rows.append(delete_job)

end_delete_rows = DummyOperator(task_id="end_delete_rows", dag=dag)


export_raw_table_tasks = []
for table, params in RAW_TABLES.items():
    task = bigquery_job_task(dag, f"import_{table}_in_raw", params)
    export_raw_table_tasks.append(task)

end_raw = DummyOperator(task_id="end_raw", dag=dag)

export_clean_table_tasks = []
for table, params in CLEAN_TABLES.items():
    task = bigquery_job_task(dag, f"import_{table}_in_clean", params)
    export_clean_table_tasks.append(task)

end_clean = DummyOperator(task_id="end_clean", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

(
    start
    >> export_tmp_table_tasks
    >> end_export_tmp_tables
    >> delete_rows
    >> end_delete_rows
    >> export_raw_table_tasks
    >> end_raw
    >> export_clean_table_tasks
    >> end_clean
    >> end
)
(end_clean >> end)
