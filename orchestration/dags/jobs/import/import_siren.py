import datetime

from common import macros
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
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator

FUNCTION_NAME = f"siren_import_{ENV_SHORT_NAME}"
SIREN_FILENAME = "siren_data.csv"
schedule_interval = "0 */6 * * *" if ENV_SHORT_NAME == "prod" else "30 */6 * * *"

DAG_NAME = "import_siren_v1"
GCE_INSTANCE = f"import-siren-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/siren"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2021, 8, 25),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import Siren from INSEE API",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule(schedule_interval),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
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
        python_version="3.8",
        base_dir=BASE_PATH,
        retries=2,
    )

    siren_to_bq = SSHGCEOperator(
        task_id="siren_to_bq",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    start = EmptyOperator(task_id="start", dag=dag)

    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> siren_to_bq
        >> gce_instance_stop
        >> end
    )
