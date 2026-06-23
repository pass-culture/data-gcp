import datetime

from airflow import DAG
from airflow.models import Param
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

DAG_NAME = "import_qualtrics"
GCE_INSTANCE = f"import-qualtrics-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/qualtrics"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}
default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 21),
    "retries": 1,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Import qualtrics tables",
    schedule=get_airflow_schedule("0 0 * * 1"),  # execute each Monday at midnight
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-4",
            type="string",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        instance_type="{{ params.instance_type }}",
        labels={"job_type": "long_task", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        base_dir=BASE_PATH,
        python_version="3.13",
        retries=2,
    )

    import_opt_out_to_bigquery = SSHGCEOperator(
        task_id="import_opt_out_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py --task import_opt_out_users",
        deferrable=True,
        do_xcom_push=True,
    )

    import_all_answers_to_bigquery = SSHGCEOperator(
        task_id="import_all_answers_to_bigquery",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py --task import_all_survey_answers",
        deferrable=True,
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )
    (gce_instance_start >> fetch_install_code)
    (fetch_install_code >> import_opt_out_to_bigquery >> gce_instance_stop)
    (fetch_install_code >> import_all_answers_to_bigquery >> gce_instance_stop)
