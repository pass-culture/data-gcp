import os
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule, sparkql_health_check

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-wikidata-dump-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"

# QLever API is unstable/timing out during the day. Run every night of the
# month instead of once a month: `check_already_extracted` skips the run
# once this month's dump succeeds, turning nightly runs into automatic
# retries until it passes, without ever retrying manually during the day.
SCHEDULE_CRON = "0 3 * * *"
DAG_NAME = "artist_wikidata_dump"

# GCS Paths / Filenames
# Path pinned to the first day of the month (resolved at runtime via
# Jinja), not `datetime.now()` (evaluated at DAG parse time). Every nightly
# attempt of the same month reads/writes the same path, enabling idempotency.
# Format stays `YYYYMMDD` (not `YYYYMM`) to stay compatible with
# `get_last_date_from_bucket` in artist_linkage, which picks the latest
# dump via plain string sort (a shorter folder name could sort incorrectly
# against legacy `YYYYMMDD` folders from the same month).
STORAGE_PATH_TEMPLATE = (
    f"gs://{DATA_GCS_BUCKET_NAME}/dump_wikidata/"
    + "{{ data_interval_start.strftime('%Y%m01') }}"
)
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

default_args = {
    "start_date": datetime(2024, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Artist extraction from wikidata",
    schedule=get_airflow_schedule(SCHEDULE_CRON),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8",
            type="string",
        ),
    },
) as dag:
    with TaskGroup("dag_init") as dag_init:
        # Check QLever (Fribourg university) server availability
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

    def _is_extraction_missing(**context) -> bool:
        """Idempotency check: skip the run if this month's dump already exists."""
        storage_path = context["task"].render_template(STORAGE_PATH_TEMPLATE, context)
        gcs_hook = GCSHook()
        bucket_name, blob_prefix = _parse_gcs_url(storage_path)
        blob_name = os.path.join(blob_prefix, WIKIDATA_EXTRACTION_GCS_FILENAME)
        already_extracted = gcs_hook.exists(
            bucket_name=bucket_name, object_name=blob_name
        )
        if already_extracted:
            print(
                f"Extraction {blob_name} already exists for this month, skipping run."
            )
        return not already_extracted

    check_already_extracted = ShortCircuitOperator(
        task_id="check_already_extracted",
        python_callable=_is_extraction_missing,
    )

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            preemptible=False,
            labels={"dag_name": DAG_NAME},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=BASE_DIR,
            retries=2,
        )
        gce_instance_start >> fetch_install_code

    extract_from_wikidata = SSHGCEOperator(
        task_id="extract_from_wikidata",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=(
            "\n uv run python cli/extract_from_wikidata.py \\\n"
            "--output-file-path "
            + os.path.join(STORAGE_PATH_TEMPLATE, WIKIDATA_EXTRACTION_GCS_FILENAME)
            + "\n"
        ),
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=GCE_INSTANCE,
        # Skip instead of failing if the check short-circuited (no VM started)
        trigger_rule="none_failed_min_one_success",
    )

    (
        dag_init
        >> check_already_extracted
        >> vm_init
        >> extract_from_wikidata
        >> gce_instance_stop
    )
