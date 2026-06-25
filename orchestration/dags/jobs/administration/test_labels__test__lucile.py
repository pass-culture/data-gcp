import datetime

from airflow import DAG
from common.callback import on_failure_base_callback
from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID
from common.operators.bigquery import BigQueryInsertJobOperatorAugmented

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "bq_labels__test__lucile"

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


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=15),
    tags=["TEST", "lucile"],
) as dag:
    run_query = BigQueryInsertJobOperatorAugmented(
        task_id="process_data",
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": "passculture-data-ehp",
                    "datasetId": "tmp_luciler",
                    "tableId": "table_with_labels",
                },
                "compression": None,
                "destinationUris": "gs://data-team-sandbox-dev/test.parquet",
                "destinationFormat": "PARQUET",
            }
        },
    )

run_query
