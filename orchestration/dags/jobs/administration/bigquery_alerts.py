import datetime

from common import macros
from common.alerts.freshness import bigquery_freshness_alert
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

GCE_INSTANCE = f"bigquery-alerts-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/bigquery_alerts"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}
DAG_NAME = "bigquery_alerts"

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_vm_callback,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Send alerts when bigquery table is not updated in expected schedule",
    schedule_interval=get_airflow_schedule(
        "00 08 * * *" if ENV_SHORT_NAME == "prod" else None
    ),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.9",
        base_dir=BASE_PATH,
    )

    wait_transfo = delayed_waiting_operator(dag=dag, external_dag_id="dbt_run_dag")

    get_warning_tables = SSHGCEOperator(
        task_id="get_warning_tables",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        do_xcom_push=True,
        command="""
        python main.py
        """,
    )

    warning_alert_slack = PythonOperator(
        task_id="warning_alert_slack",
        python_callable=bigquery_freshness_alert,
        op_kwargs={
            "warning_table_list": "{{task_instance.xcom_pull(task_ids='get_warning_tables', key='result')}}",
        },
        provide_context=True,
        dag=dag,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> wait_transfo
        >> gce_instance_start
        >> fetch_install_code
        >> get_warning_tables
        >> warning_alert_slack
        >> gce_instance_stop
    )
