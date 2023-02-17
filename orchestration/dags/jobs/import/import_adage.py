import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from common.alerts import task_fail_slack_alert
from common.operators.sensor import TimeSleepSensor
from common.utils import (
    getting_service_account_token,
    get_airflow_schedule,
)
from common import macros
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, DAG_FOLDER

from common.config import GCP_PROJECT_ID, ENV_SHORT_NAME

FUNCTION_NAME = f"adage_import_{ENV_SHORT_NAME}"
SIREN_FILENAME = "adage_data.csv"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


dag = DAG(
    "import_adage_v1",
    start_date=datetime.datetime(2020, 12, 1),
    default_args=default_dag_args,
    description="Import Adage from API",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=240),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
)

# Cannot Schedule before 5AM UTC+2 as data from API is not available.
sleep_op = TimeSleepSensor(
    dag=dag,
    task_id="sleep_task",
    execution_delay=datetime.timedelta(days=1),  # Execution Date = day minus 1
    sleep_duration=datetime.timedelta(minutes=120),  # 2H
    poke_interval=3600,  # check every hour
    mode="reschedule",
)

sa_token_op = PythonOperator(
    task_id="getting_service_account_token",
    python_callable=getting_service_account_token,
    op_kwargs={"function_name": FUNCTION_NAME},
    dag=dag,
)

adage_to_bq = SimpleHttpOperator(
    task_id="adage_to_bq",
    method="POST",
    http_conn_id="http_gcp_cloud_function",
    endpoint=FUNCTION_NAME,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer {{task_instance.xcom_pull(task_ids='getting_service_account_token', key='return_value')}}",
    },
    dag=dag,
)

end = DummyOperator(task_id="end", dag=dag)

sleep_op >> sa_token_op >> adage_to_bq >> end
