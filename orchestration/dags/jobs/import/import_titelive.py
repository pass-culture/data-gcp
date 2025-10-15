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
from common.utils import get_airflow_schedule
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
PROJECT_NAME = os.environ.get("PROJECT_NAME", "passculture-data-ehp")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "tmp_cdarnis_dev")

# Table names (variabilized by environment)
DESTINATION_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__products"
TARGET_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__products"
TEMP_TABLE = f"{PROJECT_NAME}.{BIGQUERY_DATASET}.tmp_titelive__gcs_raw"
SOURCE_TABLE_DEFAULT = (
    f"{PROJECT_NAME}.raw_{ENV_SHORT_NAME}.applicative_database_product"
)

dag_config = {
    "PROJECT_NAME": PROJECT_NAME,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
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
        "results_per_page": Param(
            default=120,
            type="integer",
            description="Number of results per page for pagination (run mode)",
        ),
        # Mode 1 (init-bq) params
        "source_table": Param(
            default=None,
            type=["null", "string"],
            description="Source BigQuery table for EAN extraction (init-bq mode)",
        ),
        "destination_table": Param(
            default=None,
            type=["null", "string"],
            description="Destination BigQuery table with batch tracking (init-bq mode)",
        ),
        "main_batch_size": Param(
            default=20000,
            type="integer",
            description="Number of EANs per batch (init-bq mode, default 20,000)",
        ),
        "sub_batch_size": Param(
            default=250,
            type="integer",
            description="Number of EANs per API call (init-bq mode, max 250)",
        ),
        "resume": Param(
            default=False,
            type="boolean",
            description="Resume from last batch_number (init-bq mode)",
        ),
        "skip_already_processed_table": Param(
            default=None,
            type=["null", "string"],
            description="Table containing already-processed EANs to skip (init-bq mode, optional)",
        ),
        "skip_count": Param(
            default=0,
            type="integer",
            description="Number of already-processed EANs to skip (init-bq mode, use with skip_already_processed_table)",
        ),
        "reprocess_failed": Param(
            default=False,
            type="boolean",
            description="Reprocess EANs with status='fail' from destination table (init-bq mode)",
        ),
        # Mode 2 (init-gcs) params
        "gcs_path": Param(
            default=None,
            type=["null", "string"],
            description="GCS file path (gs://bucket/path/file.parquet) for init-gcs mode",
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
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py init-bq "
        f"--source-table {{{{ params.source_table or '{SOURCE_TABLE_DEFAULT}' }}}} "
        f"--destination-table {{{{ params.destination_table or '{DESTINATION_TABLE}' }}}} "
        f"--main-batch-size {{{{ params.main_batch_size }}}} "
        f"--sub-batch-size {{{{ params.sub_batch_size }}}} "
        f"{{{{ '--resume' if params.resume else '' }}}} "
        f"{{{{ '--skip-already-processed-table ' + params.skip_already_processed_table if params.skip_already_processed_table else '' }}}} "
        f"{{{{ '--skip-count ' + params.skip_count|string if params.skip_already_processed_table else '' }}}} "
        f"{{{{ '--reprocess-failed' if params.reprocess_failed else '' }}}}",
    )

    # Mode 2: Init GCS - Load GCS file to BigQuery and transform
    run_init_gcs_task = SSHGCEOperator(
        task_id="run_init_gcs_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py init-gcs "
        f"--gcs-path {{{{ params.gcs_path }}}} "
        f"--temp-table {TEMP_TABLE} "
        f"--target-table {TARGET_TABLE}",
    )

    # Mode 3: Run - Incremental date range search
    run_incremental_task = SSHGCEOperator(
        task_id="run_incremental_task",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        environment=dag_config,
        command=f"PYTHONPATH={HTTP_TOOLS_RELATIVE_DIR} python main.py run "
        f"--min-modified-date {{{{ params.min_modified_date or macros.ds_add(ds, -1) }}}} "
        f"--max-modified-date {{{{ params.max_modified_date or ds }}}} "
        f"--base {{{{ params.base }}}} "
        f"--target-table {TARGET_TABLE} "
        f"--results-per-page {{{{ params.results_per_page }}}}",
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
    (gce_instance_start >> fetch_install_code >> execution_mode_branch)
    (execution_mode_branch >> run_init_bq_task >> completion_task)
    (execution_mode_branch >> run_init_gcs_task >> completion_task)
    (execution_mode_branch >> run_incremental_task >> completion_task)
    (completion_task >> gce_instance_stop)
