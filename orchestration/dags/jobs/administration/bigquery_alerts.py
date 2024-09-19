import datetime

from common import macros
from common.alerts import bigquery_freshness_alert
from common.config import (
    DAG_FOLDER,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

GCE_INSTANCE = f"bigquery-alerts-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/bigquery_alerts"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    "bigquery_alerts",
    default_args=default_dag_args,
    description="Send alerts when bigquery table is not updated in expected schedule",
    schedule_interval="00 08 * * *" if ENV_SHORT_NAME == "prod" else None,
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
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.9",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

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

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> get_warning_tables
        >> warning_alert_slack
        >> gce_instance_stop
    )
