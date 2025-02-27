import datetime

from common.alerts import on_failure_combined_callback
from common.config import (
    DE_AIRFLOW_DAG_TAG,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    VM_AIRFLOW_DAG_TAG,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param

DAG_NAME = "import_gsheet"
GCE_INSTANCE = f"import-gsheet-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/gsheet"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": on_failure_combined_callback,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Adhoc Gsheet",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    tags=[DE_AIRFLOW_DAG_TAG, VM_AIRFLOW_DAG_TAG],
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
        python_version="3.9",
        base_dir=BASE_PATH,
        retries=2,
    )

    gsheet_to_bq = SSHGCEOperator(
        task_id="gsheet_to_bq",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (gce_instance_start >> fetch_install_code >> gsheet_to_bq >> gce_instance_stop)
