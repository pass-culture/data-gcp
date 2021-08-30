import datetime
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

from google.auth.transport.requests import Request
from google.oauth2 import id_token

from dependencies.config import GCP_PROJECT, ENV_SHORT_NAME

FUNCTION_NAME = f"siren_import_{ENV_SHORT_NAME}"
SIREN_FILENAME = "siren_data.csv"

default_dag_args = {
    "start_date": datetime.datetime(2021, 8, 25),
    "retries": 1,
    "project_id": GCP_PROJECT,
}


def getting_service_account_token():
    function_url = (
        f"https://europe-west1-{GCP_PROJECT}.cloudfunctions.net/{FUNCTION_NAME}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


dag = DAG(
    "import_siren_v1",
    default_args=default_dag_args,
    description="Import Siren from INSEE API",
    on_failure_callback=None,
    schedule_interval="0 0 1 * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
)

getting_service_account_token = PythonOperator(
    task_id="getting_service_account_token",
    python_callable=getting_service_account_token,
    dag=dag,
)

siren_to_bq = SimpleHttpOperator(
    task_id="siren_to_bq",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=FUNCTION_NAME,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
    },
    dag=dag,
)


start = DummyOperator(task_id="start", dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> getting_service_account_token >> siren_to_bq >> end
