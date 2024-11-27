import os
from datetime import datetime

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    ML_AIRFLOW_DAG_TAG,
    VM_AIRFLOW_DAG_TAG,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule, sparkql_health_check

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-wikidata-dump-{ENV_SHORT_NAME}"
GCE_INSTALLER = "uv"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
SCHEDULE_CRON = "0 3 1 * *"

# GCS Paths / Filenames
GCS_FOLDER_PATH = "dump_wikidata"
STORAGE_PATH = (
    f"gs://{DATA_GCS_BUCKET_NAME}/dump_wikidata/{datetime.now().strftime('%Y%m%d')}"
)
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

default_args = {
    "start_date": datetime(2024, 7, 16),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 5,
}


with DAG(
    "artist_wikidata_dump",
    default_args=default_args,
    description="Artist extraction from wikidata",
    schedule_interval=get_airflow_schedule(SCHEDULE_CRON),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[ML_AIRFLOW_DAG_TAG, VM_AIRFLOW_DAG_TAG],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8",
            type="string",
        ),
        "link_from_scratch": Param(
            default=True,
            type="boolean",
        ),
    },
) as dag:
    with TaskGroup("dag_init") as dag_init:
        # check fribourg uni serveur availability

        health_check_task = PythonOperator(
            task_id="health_check_task",
            python_callable=sparkql_health_check,
            op_args=[QLEVER_ENDPOINT],
            dag=dag,
        )
        logging_task = PythonOperator(
            task_id="logging_task",
            python_callable=lambda: print(
                f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
            ),
            dag=dag,
        )
        health_check_task >> logging_task

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            labels={"job_type": "ml"},
            preemptible=False,
        )
        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=BASE_DIR,
            retries=2,
            installer=GCE_INSTALLER,
        )
        gce_instance_start >> fetch_install_code

    extract_from_wikidata = SSHGCEOperator(
        task_id="extract_from_wikidata",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        installer=GCE_INSTALLER,
        command=f"""
             python extract_from_wikidata.py \
            --output-file-path {os.path.join(STORAGE_PATH, WIKIDATA_EXTRACTION_GCS_FILENAME)}
            """,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    (dag_init >> vm_init >> extract_from_wikidata >> gce_instance_stop)
