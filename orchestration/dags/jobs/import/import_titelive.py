"""Airflow DAG for Titelive ETL Pipeline with multiple execution modes."""

import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import BranchPythonOperator
from common import macros
from common.alerts.task_fail import task_fail_slack_alert
from common.config import DAG_FOLDER, DAG_TAGS, ENV_SHORT_NAME
from common.operators.kubernetes import (
    DEFAULT_CONTAINER_RESOURCES,
    CustomKubernetesPodOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "import_titelive"
MICROSERVICE_PATH = "jobs/etl_jobs/external/titelive"
SPARSE_PATHS = [MICROSERVICE_PATH, "jobs/etl_jobs/http_tools"]
MAIN_SCRIPT = "main.py"

PRIORITY_WEIGHT = 1000
WEIGHT_RULE = "absolute"

default_dag_args = {
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 2,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Titelive ETL pipeline with multiple execution modes",
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value, DAG_TAGS.POD.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
            description="Git branch to deploy",
        ),
        "init": Param(
            default=False,
            type="boolean",
            description="If True, run init mode (BigQuery EAN batch). If False, run incremental mode (sync since last sync date)",
        ),
        "resume": Param(
            default=False,
            type="boolean",
            description="Resume from last batch_number (init mode)",
        ),
        "reprocess_failed": Param(
            default=False,
            type="boolean",
            description="Reprocess EANs with status='failed' from destination table (init mode)",
        ),
        "download_images_reprocess_failed": Param(
            default=False,
            type="boolean",
            description="Reprocess EANs with images_download_status='failed' (download-images step)",
        ),
    },
) as dag:

    def decide_execution_mode(**context):
        """Determine which execution mode task to run."""
        return (
            [run_init_task.task_id]
            if context["params"].get("init", False)
            else [wait_for_raw.task_id]
        )

    execution_mode_branch = BranchPythonOperator(
        task_id="decide_execution_mode",
        python_callable=decide_execution_mode,
    )

    wait_for_raw = delayed_waiting_operator(
        dag=dag,
        external_dag_id="import_applicative_database",
    )

    _kpo_common = {
        "orchestration_mode": "celery",
        "queue": "k8s-watcher",
        "runtime_mode": "gitsynced",
        "runtime_branch": "{{ params.branch }}",
        "runtime_image": "py313",
        "runtime_image_tag": "v1",
        "microservice_path": MICROSERVICE_PATH,
        "runtime_sparse_paths": SPARSE_PATHS,
        "runtime_workdir": MICROSERVICE_PATH,
        "container_resources": DEFAULT_CONTAINER_RESOURCES,
        "priority_weight": PRIORITY_WEIGHT,
        "weight_rule": WEIGHT_RULE,
        "env_vars": {"PYTHONPATH": "/app/jobs/etl_jobs"},
    }

    run_init_task = CustomKubernetesPodOperator(
        task_id="run_init_task",
        arguments=[
            MAIN_SCRIPT,
            "run-init",
            "{{ '--resume' if params.resume else '' }}",
            "{{ '--reprocess-failed' if params.reprocess_failed else '' }}",
        ],
        deferrable=True,
        **_kpo_common,
    )

    run_incremental_task = CustomKubernetesPodOperator(
        task_id="run_incremental_task",
        arguments=[MAIN_SCRIPT, "run-incremental"],
        **_kpo_common,
    )

    download_images_init = CustomKubernetesPodOperator(
        task_id="download_images_init",
        arguments=[
            MAIN_SCRIPT,
            "download-images",
            "{{ '--reprocess-failed' if params.download_images_reprocess_failed else '' }}",
        ],
        deferrable=True,
        **_kpo_common,
    )

    download_images_incremental = CustomKubernetesPodOperator(
        task_id="download_images_incremental",
        arguments=[
            MAIN_SCRIPT,
            "download-images",
            "{{ '--reprocess-failed' if params.download_images_reprocess_failed else '' }}",
        ],
        **_kpo_common,
    )

    # Task dependencies
    execution_mode_branch >> run_init_task >> download_images_init
    (
        execution_mode_branch
        >> wait_for_raw
        >> run_incremental_task
        >> download_images_incremental
    )
