import ast
import os
from datetime import datetime, timedelta

from airflow import DAG, AirflowException, settings
from airflow.contrib.operators.bigquery_operator import (
    BigQueryCreateEmptyTableOperator,
    BigQueryOperator,
)
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models.connection import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.access_gcp_secrets import access_secret_data
from dependencies.bigquery_client import BigQueryClient
from dependencies.matomo_client import MatomoClient
from dependencies.matomo_data_schema_september import (
    PROD_TABLE_DATA,
    STAGING_TABLE_DATA,
)
from dependencies.slack_alert import task_fail_slack_alert

ENV = os.environ.get("ENV")
GCP_PROJECT = os.environ.get("GCP_PROJECT")
DATA_GCS_BUCKET_NAME = os.environ.get("DATA_GCS_BUCKET_NAME")
BIGQUERY_RAW_DATASET = os.environ.get(f"BIGQUERY_RAW_DATASET")
BIGQUERY_CLEAN_DATASET = os.environ.get(f"BIGQUERY_CLEAN_DATASET")
TABLE_DATA = STAGING_TABLE_DATA if ENV == "dev" else PROD_TABLE_DATA
LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 10026


matomo_secret_id = (
    "matomo-connection-data-stg" if ENV == "dev" else "matomo-connection-data-prod"
)

MATOMO_CONNECTION_DATA = ast.literal_eval(
    access_secret_data(GCP_PROJECT, matomo_secret_id)
)
SSH_CONN_ID = "ssh_scalingo"

os.environ[
    "AIRFLOW_CONN_MYSQL_SCALINGO"
] = f"mysql://{MATOMO_CONNECTION_DATA.get('user')}:{MATOMO_CONNECTION_DATA.get('password')}@{LOCAL_HOST}:{LOCAL_PORT}/{MATOMO_CONNECTION_DATA.get('dbname')}"

matomo_client = MatomoClient(MATOMO_CONNECTION_DATA, LOCAL_PORT)

bigquery_client = BigQueryClient()


try:
    conn = BaseHook.get_connection(SSH_CONN_ID)
except AirflowException:
    conn = Connection(
        conn_id=SSH_CONN_ID,
        conn_type="ssh",
        host="ssh.osc-fr1.scalingo.com",
        login="git",
        port=22,
        extra=access_secret_data(GCP_PROJECT, "scalingo-private-key"),
    )

    session = settings.Session()
    session.add(conn)
    session.commit()


def query_mysql_from_tunnel(**kwargs):
    tunnel = matomo_client.create_tunnel()
    tunnel.start()

    extraction_task = MySqlToGoogleCloudStorageOperator(
        task_id=f"dump_{kwargs['table']}",
        sql=kwargs["sql_query"],
        bucket=DATA_GCS_BUCKET_NAME,
        filename=kwargs["file_name"],
        mysql_conn_id="mysql_scalingo",
        google_cloud_storage_conn_id="google_cloud_default",
        gzip=False,
        export_format="csv",
        field_delimiter=",",
        schema=None,
        dag=dag,
    )
    extraction_task.execute(context=kwargs)
    tunnel.stop()
    return


def query_table_data(**kwargs):
    task_parameters = STAGING_TABLE_DATA if ENV == "dev" else PROD_TABLE_DATA

    table_name = kwargs["table_name"]

    min_id = task_parameters[table_name]["min_id"]
    max_id = task_parameters[table_name]["max_id"]
    query_filter = task_parameters[table_name]["query_filter"]

    row_number_queried = TABLE_DATA[table_name]["row_number_queried"]
    query_count = 0
    for query_index in range(min_id, max_id, row_number_queried):
        sql_query = (
            f"select {', '.join([column['name'] for column in TABLE_DATA[table_name]['columns']])} "
            f"from {table_name} "
            f"where {TABLE_DATA[table_name]['id']} > {query_index} "
            f"and {TABLE_DATA[table_name]['id']} <= {query_index + row_number_queried}"
        )

        sql_query += f" and {query_filter};" if query_filter else ";"

        # File path and name.
        file_name = f"dump_scalingo/history_september/{table_name}/{now}_{query_count}_{'{}'}.csv"

        export_table_query = PythonOperator(
            task_id=f"query_{table_name}_{query_count}",
            python_callable=query_mysql_from_tunnel,
            op_kwargs={
                "table": table_name,
                "sql_query": sql_query,
                "file_name": file_name,
            },
            dag=dag,
        )
        export_table_query.execute(
            context={
                "table_name": table_name,
                "sql_query": sql_query,
                "file_name": file_name,
            }
        )
        query_count += 1


default_args = {
    "start_date": datetime(2021, 3, 18),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "import_matomo_history_v1",
    default_args=default_args,
    description="Dump scalingo matomo history data to cloud storage in csv format and use it to load it in bigquery",
    schedule_interval="@once",
    on_failure_callback=task_fail_slack_alert,
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

start = DummyOperator(task_id="start", dag=dag)
end_export = DummyOperator(task_id="end_export", dag=dag)
end_import = DummyOperator(task_id="end_import", dag=dag)


last_task = start
now = datetime.now().strftime("%Y-%m-%d")

for table in TABLE_DATA:
    export_table = PythonOperator(
        task_id=f"query_{table}",
        python_callable=query_table_data,
        op_kwargs={
            "table_name": table,
        },
        dag=dag,
    )
    last_task >> export_table
    last_task = export_table

last_task >> end_export

for table in TABLE_DATA:

    if table in ["log_visit", "log_conversion"]:
        delete_temp_table_task = BigQueryTableDeleteOperator(
            task_id=f"delete_temp_{table}_in_bigquery",
            deletion_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_RAW_DATASET}.temp_{table}",
            ignore_if_missing=True,
            dag=dag,
        )
        create_empty_table_task = BigQueryCreateEmptyTableOperator(
            task_id=f"create_empty_{table}_in_bigquery",
            project_id=GCP_PROJECT,
            dataset_id=BIGQUERY_RAW_DATASET,
            table_id=f"temp_{table}",
            schema_fields=TABLE_DATA[table]["columns"],
            dag=dag,
        )
        import_task = GoogleCloudStorageToBigQueryOperator(
            task_id=f"import_temp_{table}_in_bigquery",
            bucket=DATA_GCS_BUCKET_NAME,
            source_objects=[f"dump_scalingo/history_september/{table}/{now}_*.csv"],
            destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.temp_{table}",
            write_disposition="WRITE_EMPTY",
            skip_leading_rows=1,
            schema_fields=TABLE_DATA[table]["columns"],
            autodetect=False,
            dag=dag,
        )
        if table == "log_visit":
            delete_filter = f"WHERE idvisit IN (SELECT idvisit from {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.temp_{table})"
        if table == "log_conversion":
            delete_filter = f"WHERE CONCAT(idvisit, idgoal, buster) IN (SELECT CONCAT(idvisit, idgoal, buster) from {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.temp_{table})"
        delete_old_rows = BigQueryOperator(
            task_id=f"delete_old_{table}_rows",
            sql=f"DELETE FROM {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table} "
            + delete_filter,
            use_legacy_sql=False,
            dag=dag,
        )

        add_new_rows = BigQueryOperator(
            task_id=f"add_new_{table}_rows",
            sql=f"SELECT * from {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table} "
            f"UNION ALL (SELECT * from {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.temp_{table})",
            destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.{table}",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            dag=dag,
        )
        end_delete_temp_table_task = BigQueryTableDeleteOperator(
            task_id=f"end_delete_temp_{table}_in_bigquery",
            deletion_dataset_table=f"{GCP_PROJECT}:{BIGQUERY_RAW_DATASET}.temp_{table}",
            ignore_if_missing=True,
            dag=dag,
        )
        (
            end_export
            >> delete_temp_table_task
            >> create_empty_table_task
            >> import_task
            >> delete_old_rows
            >> add_new_rows
            >> end_delete_temp_table_task
            >> end_import
        )
    else:
        import_task = GoogleCloudStorageToBigQueryOperator(
            task_id=f"import_{table}_in_bigquery",
            bucket=DATA_GCS_BUCKET_NAME,
            source_objects=[f"dump_scalingo/history_september/{table}/{now}_*.csv"],
            destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.{table}",
            write_disposition="WRITE_TRUNCATE" if table == "goal" else "WRITE_APPEND",
            skip_leading_rows=1,
            schema_fields=[
                (
                    column
                    if column["name"] not in ["hash"]
                    else {**column, "name": f"_{column['name']}"}
                )
                for column in TABLE_DATA[table]["columns"]
            ],
            autodetect=False,
            dag=dag,
        )
        end_export >> import_task >> end_import
