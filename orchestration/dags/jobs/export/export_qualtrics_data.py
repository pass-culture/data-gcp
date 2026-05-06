import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.access_gcp_secrets import access_secret_data
from common.callback import on_failure_vm_callback
from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
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

DAG_NAME = "export_qualtrics_data"
GCE_INSTANCE = f"export-qualtrics-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/qualtrics"

QUALTRICS_MAILINGLIST_JEUNES = access_secret_data(
    GCP_PROJECT_ID, f"qualtrics_ir_jeunes_automation_id_{ENV_SHORT_NAME}"
)
QUALTRICS_MAILINGLIST_AC = access_secret_data(
    GCP_PROJECT_ID, f"qualtrics_ir_ac_automation_id_{ENV_SHORT_NAME}"
)
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
    "APPLICATIVE_EXTERNAL_CONNECTION_ID": APPLICATIVE_EXTERNAL_CONNECTION_ID,
}

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_vm_callback,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Export user data to Qualtrics mailing lists",
    schedule=get_airflow_schedule("00 06 25 * *"),
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

    export_beneficiary = SSHGCEOperator(
        task_id="export_beneficiary_to_qualtrics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command=f"""uv run python main.py \
            --task export_beneficiary \
            --ds {{{{ ds }}}} \
            --mailing-list-id {QUALTRICS_MAILINGLIST_JEUNES}
        """,
        deferrable=True,
        do_xcom_push=True,
    )

    export_venue = SSHGCEOperator(
        task_id="export_venue_to_qualtrics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command=f"""uv run python main.py \
            --task export_venue \
            --ds {{{{ ds }}}} \
            --mailing-list-id {QUALTRICS_MAILINGLIST_AC}
        """,
        deferrable=True,
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        gce_instance_start
        >> fetch_install_code
        >> [export_beneficiary, export_venue]
        >> gce_instance_stop
    )
