import datetime
import os
from common.callback import on_failure_base_callback
from common.config import (
    BIGQUERY_APPLICATIVE_SNAPSHOT_DATASET,
    BIGQUERY_INTERMEDIATE_SNAPSHOT_DATASET,
    GCP_REGION,
    DAG_TAGS,
    DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_TARGET,
    PATH_TO_DBT_PROJECT,
)
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
    get_tables_config_dict,
)

from common.dbt.dag_utils import get_models_schedule_from_manifest
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException


SNAPSHOTED_APPLICATIVE_PATH = f"{PATH_TO_DBT_PROJECT}/snapshots/raw_applicative"
SNAPSHOTED_INTERMEDIATE_PATH = f"{PATH_TO_DBT_PROJECT}/snapshots/intermediate"

SNAPSHOTED_MODELS = [
    filename[:-4]
    for path in [SNAPSHOTED_APPLICATIVE_PATH, SNAPSHOTED_INTERMEDIATE_PATH]
    for filename in os.listdir(path)
    if filename.endswith(".sql")
]
MODELS_SCHEDULES = get_models_schedule_from_manifest(
    SNAPSHOTED_MODELS, PATH_TO_DBT_TARGET, resource_type="snapshot"
)

SNAPSHOT_TABLES = get_tables_config_dict(
    PATH=SNAPSHOTED_APPLICATIVE_PATH,
    BQ_DATASET=BIGQUERY_APPLICATIVE_SNAPSHOT_DATASET,
    is_source=True,
    dbt_alias=True,
) | get_tables_config_dict(
    PATH=SNAPSHOTED_INTERMEDIATE_PATH,
    BQ_DATASET=BIGQUERY_INTERMEDIATE_SNAPSHOT_DATASET,
    is_source=True,
    dbt_alias=True,
)

GCS_SNAPSHOT_BACKUP_FOLDER = "bigquery_snapshot_backup"
GCS_SNAPSHOT_BACKUP_BUCKET = DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME


def generate_export_query(alias: str, dataset: str, yyyymmdd: str) -> str:
    return f"""
        EXPORT DATA OPTIONS(
            uri='gs://{GCS_SNAPSHOT_BACKUP_BUCKET}/{GCS_SNAPSHOT_BACKUP_FOLDER}/{alias}/{yyyymmdd}/*.parquet',
            format='PARQUET',
            compression='SNAPPY',
            overwrite=true
        ) AS
        WITH schema_info AS (
            SELECT
                TO_JSON_STRING(
                    ARRAY_AGG(
                        STRUCT(column_name, data_type)
                        ORDER BY ordinal_position
                    )
                ) AS schema_json
            FROM `{GCP_PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{alias}'
        )
        SELECT
            CURRENT_TIMESTAMP() AS backup_at,
            schema_info.schema_json AS snapshot_schema,
            TO_JSON_STRING(t) AS row_values
        FROM `{GCP_PROJECT_ID}.{dataset}.{alias}` t
        CROSS JOIN schema_info
    """


def skip_if_not_scheduled(**context):
    """
    Skip the task based on the dbt manifest data for this table.
    Expects 'table_config' in params with optional 'tags' or 'schedule'.
    """
    schedules = context["params"].get(
        "schedule_config", {}
    )  # list of tags or schedules
    ds = context["ds"]  # execution date as YYYY-MM-DD
    execution_date = datetime.datetime.strptime(ds, "%Y-%m-%d")

    if "weekly" in schedules and "monthly" in schedules:
        if execution_date.weekday() != 0 and execution_date.day != 1:
            raise AirflowSkipException(
                f"Skipping because table is weekly and monthly scheduled"
            )
    # check weekly
    elif "weekly" in schedules:
        if execution_date.weekday() != 0:  # Monday
            raise AirflowSkipException(f"Skipping because table is weekly scheduled")
    # check monthly
    elif "monthly" in schedules:
        if execution_date.day != 1:
            raise AirflowSkipException(f"Skipping because table is monthly scheduled")


default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 6,
    "on_failure_callback": on_failure_base_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag_id = "bigquery_snapshot_backup"
dag = DAG(
    dag_id,
    default_args=default_dag_args,
    dagrun_timeout=datetime.timedelta(minutes=480),
    description="historize applicative database current state to gcs bucket",
    schedule_interval=get_airflow_schedule(
        SCHEDULE_DICT.get(dag_id, {}).get(ENV_SHORT_NAME)
    ),
    catchup=False,
    tags=[DAG_TAGS.DE.value],
)

start = EmptyOperator(task_id="start", dag=dag)

with TaskGroup(group_id="snapshots_to_gcs", dag=dag) as to_gcs:
    for table_name, bq_config in SNAPSHOT_TABLES.items():
        alias = bq_config["table_alias"]
        dataset = bq_config["source_dataset"]
        _now = datetime.datetime.now()
        historization_date = _now - datetime.timedelta(days=1)
        yyyymmdd = historization_date.strftime("%Y%m%d")

        skip_task = PythonOperator(
            task_id=f"skip_check_{table_name}",
            python_callable=skip_if_not_scheduled,
            provide_context=True,
            params={**bq_config, **(MODELS_SCHEDULES.get(table_name, {}))},
            dag=dag,
        )

        waiting_task = delayed_waiting_operator(
            dag,
            external_dag_id="dbt_run_dag",
            external_task_id=f"snapshots.{table_name}",
        )

        transform_and_export = BigQueryInsertJobOperator(
            task_id=f"transform_and_export_{table_name}",
            configuration={
                "query": {
                    "query": generate_export_query(alias, dataset, yyyymmdd),
                    "useLegacySql": False,
                }
            },
            location=GCP_REGION,
            dag=dag,
        )
        skip_task >> waiting_task >> transform_and_export

end = EmptyOperator(task_id="end", dag=dag, trigger_rule="none_failed_min_one_success")

start >> to_gcs >> end
