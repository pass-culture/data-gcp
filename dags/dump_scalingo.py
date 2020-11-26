from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dependencies.data_scalingo.dump_scalingo import dump_scalingo

GCS_BUCKET = "europe-west1-data-composer-0f2400f5-bucket"
FILENAME = "file/export_tbl1/tbl1_{}.csv"
SQL_QUERY = "select * from offer;"

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

# Would be nice but issue with ssh tunnel
# t1 = PostgresToGoogleCloudStorageOperator(
#     task_id="dump_data",
#     sql=SQL_QUERY,
#     bucket=GCS_BUCKET,
#     filename=FILENAME,
#     postgres_conn_id="postgres_default",
#     google_cloud_storage_conn_id="google_cloud_default",
#     gzip=False,
#     export_format="csv",
#     field_delimiter=",",
#     schema=None,
#     dag=dag,
# )


t1 = PythonOperator(task_id="restore", python_callable=dump_scalingo, dag=dag)