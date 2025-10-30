"""Airflow DAG for Titelive ETL Pipeline with multiple execution modes."""

import datetime
import os

from common import macros
from common.callback import on_failure_vm_callback
from common.config import DAG_FOLDER, DAG_TAGS, ENV_SHORT_NAME
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule, delayed_waiting_operator
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

DAG_NAME = "import_titelive"
GCE_INSTANCE = f"import-titelive-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/etl_jobs/external/titelive"
HTTP_TOOLS_RELATIVE_DIR = "../../"

# Environment Configuration
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "tmp_cdarnis_dev")

dag_config = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
    "BIGQUERY_DATASET": BIGQUERY_DATASET,
}


default_dag_args = {
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 2,
    "on_failure_callback": on_failure_vm_callback,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Titelive ETL pipeline with multiple execution modes",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
            description="Git branch to deploy",
        ),
        "instance_type": Param(
            default="n1-standard-4",
            enum=["n1-standard-1", "n1-standard-2", "n1-standard-4", "n1-standard-8"],
            description="GCE instance type",
        ),
        "init": Param(
            default=False,
            type="boolean",
            description="If True, run init mode (BigQuery EAN batch). If False, run incremental mode (sync since last sync date)",
        ),
        # Init mode params (when init=True)
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
        # Download images params
        "download_images_reprocess_failed": Param(
            default=False,
            type="boolean",
            description="Reprocess EANs with images_download_status='failed' (download-images step)",
        ),
    },
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        instance_type="{{ params.instance_type }}",
        preemptible=False,
        labels={"job_type": "long_task", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.12",
        base_dir=BASE_DIR,
        retries=2,
    )

    # Decide execution mode based on init parameter
    def decide_execution_mode(**context):
        """Determine which execution mode task to run."""
        return (
            "run_init_task"
            if context["params"].get("init", False)
            else "run_incremental_task"
        )

    execution_mode_branch = BranchPythonOperator(
        task_id="decide_execution_mode",
        python_callable=decide_execution_mode,
    )

    wait_for_raw = delayed_waiting_operator(
        dag=dag,
        external_dag_id="import_applicative_database",
    )

    # Init mode: Extract EANs from BigQuery and batch process
    run_init_task = SSHGCEOperator(
        task_id="run_init_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py run-init "
        f"{{{{ '--resume' if params.resume else '' }}}} "
        f"{{{{ '--reprocess-failed' if params.reprocess_failed else '' }}}}",
        deferrable=True,
    )

    # Incremental mode: Sync since last sync date for both bases
    run_incremental_task = SSHGCEOperator(
        task_id="run_incremental_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py run-incremental",
    )

    # Completion task to merge branches
    completion_task = EmptyOperator(
        task_id="completion_task",
        trigger_rule="none_failed_min_one_success",
    )

    # Download images for init mode (deferrable)
    download_images_init = SSHGCEOperator(
        task_id="download_images_init",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py download-images "
        f"{{{{ '--reprocess-failed' if params.download_images_reprocess_failed else '' }}}}",
        deferrable=True,
    )

    # Download images for incremental mode (not deferrable)
    download_images_incremental = SSHGCEOperator(
        task_id="download_images_incremental",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py download-images "
        f"{{{{ '--reprocess-failed' if params.download_images_reprocess_failed else '' }}}}",
        deferrable=False,
    )

    # VM cleanup
    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=GCE_INSTANCE,
        trigger_rule="none_failed_min_one_success",
    )

    # Task dependencies
    (gce_instance_start >> fetch_install_code >> execution_mode_branch)

    (
        execution_mode_branch
        >> run_init_task
        >> completion_task
        >> download_images_init
        >> gce_instance_stop
    )
    (
        execution_mode_branch
        >> wait_for_raw
        >> run_incremental_task
        >> completion_task
        >> download_images_incremental
        >> gce_instance_stop
    )
