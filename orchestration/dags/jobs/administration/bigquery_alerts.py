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
from common.utils import delayed_waiting_operator, get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator

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

# Default environment-specific configuration
DEFAULT_ENV_CONFIG = {
    "prod": {
        "archive_days": 28,
        "delete_instead_of_archive": False,
        "skip_slack_alerts": False,
        "archive_dataset": "sandbox_prod",
    },
    "stg": {
        "archive_days": 7,
        "delete_instead_of_archive": False,
        "skip_slack_alerts": True,
        "archive_dataset": "sandbox_prod",
    },
    "dev": {
        "archive_days": 7,
        "delete_instead_of_archive": False,
        "skip_slack_alerts": True,
        "archive_dataset": "sandbox_prod",
    },
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
        "archive_days": Param(
            default=DEFAULT_ENV_CONFIG[ENV_SHORT_NAME]["archive_days"],
            type="integer",
            description="Number of days before archiving/deleting a table",
        ),
        "delete_instead_of_archive": Param(
            default=DEFAULT_ENV_CONFIG[ENV_SHORT_NAME]["delete_instead_of_archive"],
            type="boolean",
            description="Delete tables instead of archiving them",
        ),
        "skip_slack_alerts": Param(
            default=DEFAULT_ENV_CONFIG[ENV_SHORT_NAME]["skip_slack_alerts"],
            type="boolean",
            description="Skip sending Slack alerts",
        ),
        "archive_dataset": Param(
            default=DEFAULT_ENV_CONFIG[ENV_SHORT_NAME]["archive_dataset"],
            type=["string", "null"],
            description="Dataset to archive tables to",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.12",
        base_dir=BASE_PATH,
    )

    wait_transfo = delayed_waiting_operator(dag=dag, external_dag_id="dbt_run_dag")

    # Build command parts
    base_command = "python main.py"
    archive_days = "--archive-days {{ params.archive_days }}"
    delete_flag = "{{ '--delete-instead-of-archive' if params.delete_instead_of_archive else '' }}"
    slack_config = "{{ '--skip-slack-alerts' if params.skip_slack_alerts else '' }}"
    archive_dataset = "{{ f'--archive-dataset {{ params.archive_dataset }}' if params.archive_dataset else '' }}"

    # Combine all parts
    command = (
        f"{base_command} {archive_days} {delete_flag} {slack_config} {archive_dataset}"
    )

    get_warning_tables = SSHGCEOperator(
        task_id="get_warning_tables",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command=command,
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
        >> gce_instance_stop
    )
