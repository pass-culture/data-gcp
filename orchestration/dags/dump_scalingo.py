import os
import ast
from datetime import datetime, timedelta

import airflow
import gcsfs
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.gcp_sql_operator import (
    CloudSqlQueryOperator,
    CloudSqlInstanceImportOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

#  Global variables
GCS_BUCKET = "dump_scalingo"
GCP_PROJECT_ID = "pass-culture-app-projet-test"

TABLES = [
    "user",
    "provider",
    "offerer",
    "bank_information",
    "booking",
    "payment",
    "venue",
    "user_offerer",
    "offer",
    "stock",
    "favorite",
    "venue_type",
    "venue_label",
]

TESTING = ast.literal_eval(os.environ.get("TESTING"))
LOCAL_HOST = "localhost"
LOCAL_PORT = 10025

DATABASE = "test-restore"
INSTANCE_DATABASE = "dump-prod-8-10-2020"

# Starting DAG
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "temp_dump_prod_from_scalingo_v2",
    default_args=default_args,
    start_date=datetime(2020, 12, 8),
    description="Dump scalingo db to cloud storage in csv format",
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)


start = DummyOperator(task_id="start", dag=dag)


def create_tunnel():
    # Open SSH tunnel
    ssh_hook = SSHHook(
        ssh_conn_id="ssh_scalingo",
        keepalive_interval=1200,
    )
    tunnel = ssh_hook.get_tunnel(
        remote_port=TESTING["port"],
        remote_host=TESTING["host"],
        local_port=LOCAL_PORT,
    )
    return tunnel


def query_postgresql_from_tunnel(**kwargs):
    tunnel = create_tunnel()
    tunnel.start()

    export_table = PostgresToGoogleCloudStorageOperator(
        task_id=f"dump_{kwargs['table']}",
        sql=kwargs["sql_query"],
        bucket=GCS_BUCKET,
        schema_filename=None,
        filename=kwargs["file_name"],
        postgres_conn_id="postgres_scalingo",
        google_cloud_storage_conn_id="google_cloud_default",
        gzip=False,
        export_format="csv",
        field_delimiter=",",
        schema=None,
        dag=dag,
    )

    export_table.execute(context=kwargs)
    tunnel.stop()
    return


def clean_csv(file_name):
    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
    with fs.open(f"gs://{GCS_BUCKET}/{file_name}") as file_in:
        with fs.open(f"gs://{GCS_BUCKET}/{file_name}", "w") as file_out:
            for line in file_in.readlines()[1:]:
                file_out.write(
                    line.decode("utf-8")
                    .replace("[", "{")
                    .replace("]", "}")
                    .replace("null", "")
                )


last_task = start

for table in TABLES:
    sql_query = f"select * from {table};"

    # File path and name.
    now = datetime.now()
    file_name = f"{table}/{now.year}_{now.month}_{now.day}_{table}.csv"

    export_table = PythonOperator(
        task_id=f"query_{table}",
        python_callable=query_postgresql_from_tunnel,
        op_kwargs={"table": table, "sql_query": sql_query, "file_name": file_name},
        dag=dag,
    )

    last_task >> export_table
    last_task = export_table


for table in TABLES:
    # File path and name.
    now = datetime.now()
    file_name = f"{table}/{now.year}_{now.month}_{now.day}_{table}.csv"

    clean_table = PythonOperator(
        task_id=f"clean_csv_{table}",
        python_callable=clean_csv,
        op_kwargs={"file_name": file_name},
        dag=dag,
    )

    drop_table_task = CloudSqlQueryOperator(
        gcp_cloudsql_conn_id="test_cloudsql",
        task_id=f"drop_table_public_{table}",
        sql=f"DELETE FROM public.{table};",
        autocommit=True,
    )

    import_body = {
        "importContext": {
            "fileType": "CSV",
            "csvImportOptions": {"table": f"public.{table}"},
            "uri": f"gs://{GCS_BUCKET}/{file_name}",
            "database": DATABASE,
        }
    }

    sql_restore_task = CloudSqlInstanceImportOperator(
        task_id=f"cloud_sql_restore_table_{table}",
        project_id=GCP_PROJECT_ID,
        body=import_body,
        instance=INSTANCE_DATABASE,
    )

    last_task >> clean_table >> drop_table_task >> sql_restore_task
    last_task = sql_restore_task


end = DummyOperator(task_id="end", dag=dag)
last_task >> end
