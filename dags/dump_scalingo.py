from datetime import timedelta
import airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


gcs_bucket = "dump_scalingo"
tables = [
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
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "dump_scalingo",
    default_args=default_args,
    description="Dump scalingo db to csv",
    schedule_interval="@once",
    dagrun_timeout=timedelta(minutes=20),
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

ssh_port = 22
ssh_username = "git"
ssh_hostname = "ssh.osc-fr1.scalingo.com"


def open_tunnel():
    # Open SSH tunnel
    ssh_hook = SSHHook(
        ssh_conn_id="ssh_scalingo",
        keepalive_interval=120,
    )
    tunnel = ssh_hook.get_tunnel(
        remote_port=TESTING["port"],
        remote_host=TESTING["host"],
        local_port=10025,
    )
    tunnel.start()

    return


open_tunnel = PythonOperator(
    task_id="test_tunnel_conn", python_callable=open_tunnel, dag=dag
)

last_task = open_tunnel
for table in tables:
    sql_query = f"select * from {table};"

    # File path and name.
    now = datetime.now()
    file_name = f"{now.year}_{now.month}_{now.day}_{table}.csv"

    export_table = PostgresToGoogleCloudStorageOperator(
        task_id=f"dump_{table}",
        sql=sql_query,
        bucket=gcs_bucket,
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