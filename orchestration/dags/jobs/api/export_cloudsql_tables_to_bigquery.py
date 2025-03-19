import datetime
import os

from common import macros
from common.access_gcp_secrets import access_secret_data
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    GCP_PROJECT_ID,
    GCP_REGION,
    RECOMMENDATION_SQL_INSTANCE,
)
from common.operators.bigquery import bigquery_job_task
from common.utils import from_external, get_airflow_schedule
from dependencies.export_cloudsql_tables_to_bigquery.config import (
    PAST_OFFER_CONTEXT_RAW_QUERY,
    PAST_OFFER_CONTEXT_TMP_QUERY,
)
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
)
from airflow.utils.task_group import TaskGroup

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
CONNECTION_ID = f"{GCP_PROJECT_ID}.{GCP_REGION}.{DATABASE_INSTANCE_NAME}"
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    DATABASE_URL.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT_ID}&location={GCP_REGION}&instance={DATABASE_INSTANCE_NAME}&use_proxy=True&sql_proxy_use_tcp=True"
)


def fetch_dates_imported_in_raw(**context):
    base_table = PAST_OFFER_CONTEXT_RAW_QUERY["destination_table"].split("$")[0]
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
    SELECT DISTINCT FORMAT_DATE('%Y-%m-%d', date) as event_date
    FROM `{GCP_PROJECT_ID}.{PAST_OFFER_CONTEXT_RAW_QUERY["destination_dataset"]}.{base_table}`
    WHERE import_date = "{context["ds"]}"
    """

    print(f"Executing query: {query}")
    query_job = client.query(query)
    results = query_job.result()
    dates = [row.event_date for row in results]
    return dates


with DAG(
    "export_cloudsql_tables_to_bigquery_v2",
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

    with TaskGroup(
        "drop_past_offer_context_yesterday_rows"
    ) as drop_past_offer_context_yesterday_rows:
        fetch_dates_imported_in_raw_task = PythonOperator(
            task_id="fetch_dates_imported_in_raw_task",
            python_callable=fetch_dates_imported_in_raw,
            provide_context=True,
        )

        drop_rows_in_cloudsql = CloudSQLExecuteQueryOperator(
            task_id="drop_past_offer_context_yesterday_rows",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql="""
            DELETE FROM public.past_offer_context
            WHERE CAST(date AS DATE) <= '{{ macros.ds_add(ds, -1) }}'
            {% set dates_imported_in_raw = ti.xcom_pull(task_ids='drop_past_offer_context_yesterday_rows.fetch_dates_imported_in_raw_task') %}
            {% if dates_imported_in_raw %}
                AND CAST(date AS DATE) IN (
                    {% for d in dates_imported_in_raw %}
                        '{{ d }}'{% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
            {% endif %}
            """,
            autocommit=True,
        )

        fetch_dates_imported_in_raw_task >> drop_rows_in_cloudsql

    end = EmptyOperator(task_id="end", dag=dag)


(
    start
    >> get_past_offer_context_from_cloudsql
    >> export_past_offer_context_to_bigquery
    >> drop_past_offer_context_yesterday_rows
    >> end
)
