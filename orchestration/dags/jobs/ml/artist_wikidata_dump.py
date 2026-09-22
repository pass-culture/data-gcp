import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
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

from jobs.crons import SCHEDULE_DICT

logger = logging.getLogger(__name__)

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-wikidata-dump-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
DAG_NAME = "artist_wikidata_dump"

# Schedule (see SCHEDULE_DICT in jobs/crons.py) runs once a month. Queries are
# optimized and QLever failures are now rare enough that a nightly-retry-until-
# success schedule isn't worth the Airflow UI clutter and upkeep it costs; a
# genuine failure retries at the task level (`retries: 5` below) and, beyond
# that, can be re-run manually and scoped to just the failed target(s) via the
# `targets` param.

# GCS Paths / Filenames
# Path pinned to the first day of the month (resolved at runtime via Jinja),
# not `datetime.now()` (evaluated at DAG parse time). Uses `data_interval_end`
# (not `data_interval_start`): for this monthly schedule, `data_interval_start`
# is the *previous* month (interval is [start, end) = [month-1, month)), which
# would wrongly pin the run to the previous month. `data_interval_end` always
# matches the actual month the run is for.
# Format stays `YYYYMMDD` (not `YYYYMM`) to stay compatible with
# `get_last_date_from_bucket` in artist_linkage, which picks the latest
# dump via plain string sort (a shorter folder name could sort incorrectly
# against legacy `YYYYMMDD` folders from the same month).
STORAGE_PATH_PREFIX_TEMPLATE = (
    "dump_wikidata/{{ data_interval_end.strftime('%Y%m01') }}"
)
STORAGE_PATH_TEMPLATE = f"gs://{DATA_GCS_BUCKET_NAME}/{STORAGE_PATH_PREFIX_TEMPLATE}"
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
QLEVER_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

# One `extract` task per target, each writing its own raw file below, so a QLever
# failure on one target is isolated (its own task_id, its own retries) instead of
# forcing every other, already-fetched target to be re-fetched too.
# Must mirror the keys of QUERY_CONFIGS in
# jobs/ml_jobs/artist_linkage/src/wikidata_config.py.
EXTRACTION_TARGETS = ["music", "music_ids", "book", "movie", "gkg"]
RAW_DUMPS_GCS_PREFIX_TEMPLATE = f"{STORAGE_PATH_PREFIX_TEMPLATE}/raw"
RAW_DUMPS_PATH_TEMPLATE = f"gs://{DATA_GCS_BUCKET_NAME}/{RAW_DUMPS_GCS_PREFIX_TEMPLATE}"

# QLever is a shared, unstable public endpoint. An Airflow-level retry here means
# something disrupted it badly enough to survive cli/extract_from_wikidata.py's own
# tenacity retries *within* a single attempt (3 tries, exponential backoff up to
# 40s) — so give it real time to recover instead of coming back at Airflow's
# 5-minute default.
EXTRACT_RETRY_DELAY = timedelta(minutes=10)
EXTRACT_MAX_RETRY_DELAY = timedelta(minutes=30)

default_args = {
    "start_date": datetime(2024, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Artist extraction from wikidata",
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
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
        "targets": Param(
            default=["all"],
            type="array",
            items={"type": "string", "enum": ["all", *EXTRACTION_TARGETS]},
            examples=["all", *EXTRACTION_TARGETS],
            description="Extract every target, or just re-run one or more (e.g. to "
            "redo the targets that failed last run without re-fetching the rest). "
            "`all` takes priority over any other selection.",
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
            python_callable=lambda: logger.info(
                "Task executed for branch : %s and instance : %s on env : %s",
                dag.params.get("branch"),
                dag.params.get("instance_type"),
                ENV_SHORT_NAME,
            ),
            dag=dag,
        )
        health_check_task >> logging_task

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

    def _make_should_extract_target(target: str):
        def _should_extract_target(**context) -> bool:
            """Gate a single target's extraction on the `targets` param."""
            selected_targets = context["params"].get("targets", ["all"])
            should_run = "all" in selected_targets or target in selected_targets
            if not should_run:
                logger.info(
                    "Skipping %s extraction (targets param is %r).",
                    target,
                    selected_targets,
                )
            return should_run

        return _should_extract_target

    with TaskGroup("extract_from_wikidata") as extract_from_wikidata:
        # Chained sequentially (not parallel) so we don't hammer the shared, already
        # unstable QLever endpoint with several heavy queries at once.
        # `trigger_rule="all_done"` on each gate means one target genuinely failing
        # does not stop the others from being attempted in the same run: each raw
        # file is independent, so there's no reason a bad `book` extract should
        # block `movie`/`gkg` from still being fetched.
        extract_tasks = []
        previous_extract_task = None
        for target in EXTRACTION_TARGETS:
            should_extract_task = ShortCircuitOperator(
                task_id=f"should_extract_{target}",
                python_callable=_make_should_extract_target(target),
                ignore_downstream_trigger_rules=False,
                trigger_rule="all_done",
            )
            extract_task = SSHGCEOperator(
                task_id=f"extract_{target}",
                instance_name=GCE_INSTANCE,
                base_dir=BASE_DIR,
                retry_delay=EXTRACT_RETRY_DELAY,
                retry_exponential_backoff=True,
                max_retry_delay=EXTRACT_MAX_RETRY_DELAY,
                command=f"""
                     uv run python cli/extract_from_wikidata.py extract \
                    --query-name {target} \
                    --output-file-path {os.path.join(RAW_DUMPS_PATH_TEMPLATE, f"{target}.parquet")}
                    """,
            )
            should_extract_task >> extract_task
            if previous_extract_task is not None:
                previous_extract_task >> should_extract_task
            previous_extract_task = extract_task
            extract_tasks.append(extract_task)

    merge_wikidata_extraction = SSHGCEOperator(
        task_id="merge_wikidata_extraction",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        # Always attempt merge: it reads each target's raw file straight from its
        # per-month GCS path, not from "what this run produced", so it doesn't care
        # whether a target here succeeded, failed, or was skipped — a target that
        # fails this run but succeeded on a previous run this month still has a
        # usable (if stale) raw file. It only hard-fails (see cli/extract_from_
        # wikidata.py::merge) if a required target has never succeeded all month.
        trigger_rule="all_done",
        command=f"""
             uv run python cli/extract_from_wikidata.py merge \
            --input-dir-path {RAW_DUMPS_PATH_TEMPLATE} \
            --output-file-path {os.path.join(STORAGE_PATH_TEMPLATE, WIKIDATA_EXTRACTION_GCS_FILENAME)}
            """,
    )

    cleanup_raw_dumps = GCSDeleteObjectsOperator(
        task_id="cleanup_raw_dumps",
        bucket_name=DATA_GCS_BUCKET_NAME,
        prefix=RAW_DUMPS_GCS_PREFIX_TEMPLATE,
        # Default "all_success": only delete the raw per-target files once every
        # target genuinely succeeded *this run* (not skipped, not reused from a
        # stale file) and merge itself succeeded. A targeted/partial run, or a
        # merge that fell back to an older raw file for a failed target, leaves
        # them in place — they may still be needed for a future targeted rerun.
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=GCE_INSTANCE,
        # Always tear down the VM via the graph, whatever the outcome above —
        # don't rely solely on `on_failure_callback` to catch the failure path.
        trigger_rule="all_done",
    )

    (
        dag_init
        >> vm_init
        >> extract_from_wikidata
        >> merge_wikidata_extraction
        >> gce_instance_stop
    )
    (
        [*extract_tasks, merge_wikidata_extraction]
        >> cleanup_raw_dumps
        >> gce_instance_stop
    )
