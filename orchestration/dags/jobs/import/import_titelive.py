"""Airflow DAG for Titelive ETL Pipeline with multiple execution modes."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import DAG_FOLDER, DAG_TAGS, ENV_SHORT_NAME
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

# Basic settings
BASE_DIR = "data-gcp/jobs/etl_jobs/external/titelive"
DAG_NAME = "import_titelive"
DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"import-titelive-{ENV_SHORT_NAME}"

# Environment Configuration
PROJECT_NAME = os.environ.get("PROJECT_NAME", "passculture-data-dev")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "tmp_cdarnis_dev")

# Table names (variabilized by environment)
TARGET_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__products"
TRACKING_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__tracking"
TEMP_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__gcs_raw"
SOURCE_TABLE_DEFAULT = (
    f"{PROJECT_NAME}.raw_{ENV_SHORT_NAME}.applicative_database_product"
)


default_args = {
    "owner": "data-team",
    "start_date": datetime(2025, 1, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    DAG_NAME,
    default_args=default_args,
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
        "execution_mode": Param(
            default="run",
            enum=["init-bq", "init-gcs", "run"],
            description="Execution mode: init-bq (batch EAN), init-gcs (GCS file), run (date range)",
        ),
        # Common params
        "base": Param(
            default="paper",
            enum=["paper", "music"],
            description="Product category",
        ),
        # Mode 3 (run) params
        "min_modified_date": Param(
            default=None,
            type=["null", "string"],
            description="Min modification date (YYYY-MM-DD) - defaults to yesterday",
        ),
        "max_modified_date": Param(
            default=None,
            type=["null", "string"],
            description="Max modification date (YYYY-MM-DD) - defaults to today",
        ),
        # Mode 1 (init-bq) params
        "source_table": Param(
            default=None,
            type=["null", "string"],
            description="Source BigQuery table for EAN extraction (init-bq mode)",
        ),
        "batch_size": Param(
            default=50,
            type="integer",
            description="Batch size for EAN processing (init-bq mode)",
        ),
        # Mode 2 (init-gcs) params
        "gcs_path": Param(
            default=None,
            type=["null", "string"],
            description="GCS file path (gs://bucket/path/file.parquet) for init-gcs mode",
        ),
    },
) as dag:
    # Task Group: DAG initialization
    with TaskGroup("dag_init") as dag_init:
        logging_task = PythonOperator(
            task_id="logging_task",
            python_callable=lambda: print(
                f"Executing Titelive ETL - Mode: {dag.params.get('execution_mode')}, "
                f"Branch: {dag.params.get('branch')}, "
                f"Instance: {dag.params.get('instance_type')}, "
                f"Env: {ENV_SHORT_NAME}"
            ),
            dag=dag,
        )

    # Task Group: VM initialization
    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            preemptible=False,
            labels={"job_type": "etl_task", "dag_name": DAG_NAME},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.12",
            base_dir=BASE_DIR,
            retries=2,
        )

        gce_instance_start >> fetch_install_code

    # Branch decision based on execution mode
    def decide_execution_mode(**context):
        """Determine which execution mode task to run."""
        mode = context["params"].get("execution_mode", "run")
        if mode == "init-bq":
            return "run_init_bq_task"
        elif mode == "init-gcs":
            return "run_init_gcs_task"
        else:  # "run"
            return "run_incremental_task"

    execution_mode_branch = BranchPythonOperator(
        task_id="decide_execution_mode",
        python_callable=decide_execution_mode,
    )

    # Mode 1: Init BQ - Extract EANs from BigQuery and batch process
    run_init_bq_task = SSHGCEOperator(
        task_id="run_init_bq_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
            export PROJECT_NAME={PROJECT_NAME}
            export ENV_SHORT_NAME={ENV_SHORT_NAME}
            PYTHONPATH=. python main.py init-bq \
                --source-table {{{{ params.source_table or '{SOURCE_TABLE_DEFAULT}' }}}} \
                --tracking-table {TRACKING_TABLE} \
                --target-table {TARGET_TABLE} \
                --batch-size {{{{ params.batch_size }}}}
        """,
    )

    # Mode 2: Init GCS - Load GCS file to BigQuery and transform
    run_init_gcs_task = SSHGCEOperator(
        task_id="run_init_gcs_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
            export PROJECT_NAME={PROJECT_NAME}
            export ENV_SHORT_NAME={ENV_SHORT_NAME}
            PYTHONPATH=. python main.py init-gcs \
                --gcs-path {{{{ params.gcs_path }}}} \
                --temp-table {TEMP_TABLE} \
                --target-table {TARGET_TABLE}
        """,
    )

    # Mode 3: Run - Incremental date range search
    run_incremental_task = SSHGCEOperator(
        task_id="run_incremental_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
            export PROJECT_NAME={PROJECT_NAME}
            export ENV_SHORT_NAME={ENV_SHORT_NAME}
            PYTHONPATH=. python main.py run \
                --min-modified-date {{{{ params.min_modified_date or macros.ds_add(ds, -1) }}}} \
                --max-modified-date {{{{ params.max_modified_date or ds }}}} \
                --base {{{{ params.base }}}} \
                --target-table {TARGET_TABLE}
        """,
    )

    # Completion task to merge branches
    completion_task = EmptyOperator(
        task_id="completion_task",
        trigger_rule="none_failed_min_one_success",
    )

    # VM cleanup
    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=GCE_INSTANCE,
        trigger_rule="none_failed_min_one_success",
    )

    # Task dependencies
    dag_init >> vm_init >> execution_mode_branch

    # Branch paths
    execution_mode_branch >> run_init_bq_task >> completion_task
    execution_mode_branch >> run_init_gcs_task >> completion_task
    execution_mode_branch >> run_incremental_task >> completion_task

    # Cleanup
    completion_task >> gce_instance_stop
