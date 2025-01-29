import datetime

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, ENV_SHORT_NAME, GCP_PROJECT_ID
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param

DAG_NAME = "import_zendesk"
GCE_INSTANCE = f"import-zendesk-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/zendesk"
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import zendesk tables",
    schedule_interval=get_airflow_schedule("00 01 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "ndays": Param(
            default=7 if ENV_SHORT_NAME == "prod" else 1,
            type="integer",
        ),
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
        dag=dag,
        retries=2,
    )

    import_data_to_bigquery = SSHGCEOperator(
        task_id="import_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py --ndays {{ params.ndays }} ",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

(
    gce_instance_start
    >> fetch_install_code
    >> import_data_to_bigquery
    >> gce_instance_stop
)
