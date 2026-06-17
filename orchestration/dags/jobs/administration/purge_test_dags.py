import datetime
import fnmatch
import logging
import os

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from common.callback import on_failure_base_callback
from common.config import DAG_TAGS, ENV_SHORT_NAME, GCP_PROJECT_ID, GCS_AIRFLOW_BUCKET
from common.utils import get_airflow_schedule
from google.cloud import storage

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "purge_test_dags"

TEST_DAG_MAX_AGE_DAYS = 14
TEST_DAG_FILE_REGEX_PATTERN = "*__test__*.py"

default_dag_args = {
    "start_date": datetime.datetime(2026, 6, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
}

schedule = SCHEDULE_DICT.get(DAG_NAME, {})
if isinstance(schedule, dict):
    schedule = schedule.get(ENV_SHORT_NAME)
else:
    schedule = None


def _purge_test_dags(**context) -> None:
    force_purge_all: bool = context["params"]["force_purge_all"]
    max_age_days: int = context["params"]["max_age_days"]

    client = storage.Client()
    bucket = client.bucket(GCS_AIRFLOW_BUCKET)
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=max_age_days
    )

    if force_purge_all:
        logging.info("force_purge_all=True: deleting all test DAGs regardless of age.")
    else:
        logging.info(
            "Deleting test DAGs last updated before %s (%d-day threshold).",
            cutoff.isoformat(),
            max_age_days,
        )

    deleted = []
    for blob in client.list_blobs(bucket, prefix="dags/jobs/"):
        if not fnmatch.fnmatch(blob.name, TEST_DAG_FILE_REGEX_PATTERN):
            continue
        if blob.name.endswith(os.path.basename(__file__)):
            continue
        if force_purge_all or blob.updated < cutoff:
            logging.info("Deleting %s (last updated %s)", blob.name, blob.updated)
            blob.delete()
            deleted.append(blob.name)

    logging.info("Purged %d test DAG(s).", len(deleted))


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description=f"Delete test DAGs from GCS older than {TEST_DAG_MAX_AGE_DAYS} days (or all when force_purge_all=True)",
    schedule_interval=get_airflow_schedule(schedule),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    params={
        "force_purge_all": Param(
            default=False,
            type="boolean",
            description="When True, delete all test DAGs regardless of age. Intended for manual triggers only.",
        ),
        "max_age_days": Param(
            default=TEST_DAG_MAX_AGE_DAYS,
            type="integer",
            description="Delete test DAGs last updated more than this many days ago.",
        ),
    },
    tags=[DAG_TAGS.DE.value],
) as dag:
    purge = PythonOperator(
        task_id="purge_test_dags",
        python_callable=_purge_test_dags,
    )
