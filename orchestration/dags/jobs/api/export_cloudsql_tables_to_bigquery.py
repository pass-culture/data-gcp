import datetime
import os

from common import macros
from common.access_gcp_secrets import access_secret_data
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_TAGS,
    RECOMMENDATION_SQL_INSTANCE,
)
from common.operators.bigquery import bigquery_job_task
from common.utils import from_external, get_airflow_schedule
from dependencies.export_cloudsql_tables_to_bigquery.config import (
    PAST_OFFER_CONTEXT_RAW_QUERY,
    PAST_OFFER_CONTEXT_TMP_QUERY,
)

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LOCATION = os.environ.get("REGION")
DAG_FOLDER = os.environ.get("DAG_FOLDER")
DEFAULT_DAG_ARGS = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=60),
    "project_id": GCP_PROJECT_ID,
}

# Recreate proprely the connection url
DATABASE_INSTANCE_NAME = access_secret_data(
    GCP_PROJECT_ID, f"{RECOMMENDATION_SQL_INSTANCE}_database_instance_name", default=""
)
DATABASE_URL = access_secret_data(
    GCP_PROJECT_ID, f"{RECOMMENDATION_SQL_INSTANCE}_database_url", default=""
)
CONNECTION_ID = f"{GCP_PROJECT_ID}.{LOCATION}.{DATABASE_INSTANCE_NAME}"
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    DATABASE_URL.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT_ID}&location={LOCATION}&instance={DATABASE_INSTANCE_NAME}&use_proxy=True&sql_proxy_use_tcp=True"
)


with DAG(
    "export_cloudsql_tables_to_bigquery_v1",
    default_args=DEFAULT_DAG_ARGS,
    description="Export tables from recommendation CloudSQL to BigQuery",
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=180),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value],
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    get_past_offer_context_from_cloudsql = BigQueryInsertJobOperator(
        task_id="get_past_offer_context_from_cloudsql",
        configuration={
            "query": {
                "query": from_external(
                    conn_id=CONNECTION_ID, sql_path=PAST_OFFER_CONTEXT_TMP_QUERY["sql"]
                ),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": PAST_OFFER_CONTEXT_TMP_QUERY["destination_dataset"],
                    "tableId": PAST_OFFER_CONTEXT_TMP_QUERY["destination_table"],
                },
                "writeDisposition": PAST_OFFER_CONTEXT_TMP_QUERY["write_disposition"],
            }
        },
    )

    export_past_offer_context_to_bigquery = bigquery_job_task(
        dag=dag,
        table="export_past_offer_context_to_bigquery",
        job_params=PAST_OFFER_CONTEXT_RAW_QUERY,
    )

    drop_past_offer_context_yesterday_rows = CloudSQLExecuteQueryOperator(
        task_id="drop_past_offer_context_yesterday_rows",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql="DELETE FROM public.past_offer_context where date <= {{ macros.ds_add(ds, -1) }}",
        autocommit=True,
        dag=dag,
    )

    end = EmptyOperator(task_id="end", dag=dag)


(
    start
    >> get_past_offer_context_from_cloudsql
    >> export_past_offer_context_to_bigquery
    >> drop_past_offer_context_yesterday_rows
    >> end
)
