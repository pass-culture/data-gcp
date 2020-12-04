from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

GCS_BUCKET = "dump_scalingo"
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


default_args = {
    "start_date": datetime(2020, 12, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dump_scalingo_v1",
    default_args=default_args,
    description="Dump scalingo db to cloud storage in csv format",
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

# ENV TESTING to transfer in env var with PC-5263
TESTING = {
    "port": 34469,
    "host": "pass-culture-407.postgresql.dbs.scalingo.com",
    "dbname": "pass_culture_407",
    "user": "pass_culture_407",
    "password": "szJm55of481Jfl2LS7W3",
}
LOCAL_HOST = "localhost"
LOCAL_PORT = 10025

start = DummyOperator(task_id="start", dag=dag)


def open_tunnel():
    # Open SSH tunnel
    ssh_hook = SSHHook(
        ssh_conn_id="ssh_scalingo",
        keepalive_interval=120,
    )
    tunnel = ssh_hook.get_tunnel(
        remote_port=TESTING["port"],
        remote_host=TESTING["host"],
        local_port=LOCAL_PORT,
    )
    tunnel.start()

    return


open_tunnel = PythonOperator(
    task_id="test_tunnel_conn", python_callable=open_tunnel, dag=dag
)
start >> open_tunnel
last_task = open_tunnel

for table in TABLES:
    sql_query = f"select * from {table};"

    # File path and name.
    now = datetime.now()
    file_name = f"{table}/{now.year}_{now.month}_{now.day}_{table}.csv"

    export_table = PostgresToGoogleCloudStorageOperator(
        task_id=f"dump_{table}",
        sql=sql_query,
        bucket=GCS_BUCKET,
        filename=file_name,
        postgres_conn_id="postgres_scalingo",
        google_cloud_storage_conn_id="google_cloud_default",
        gzip=False,
        export_format="csv",
        field_delimiter=",",
        schema=None,
        dag=dag,
    )

    last_task >> export_table
    last_task = export_table

end = DummyOperator(task_id="end", dag=dag)
last_task >> end
