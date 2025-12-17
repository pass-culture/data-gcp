import datetime
import os
from common.callback import on_failure_base_callback
from common.config import (
    GCP_REGION,
    DAG_TAGS,
    DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_TARGET,
    PATH_TO_DBT_PROJECT,
)
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
)

from common.dbt.dag_utils import (
    get_models_schedule_from_manifest,
    get_node_alias_and_dataset,
    load_manifest_with_mtime,
)
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
import json
import hashlib
from google.cloud import bigquery, storage
from airflow.exceptions import AirflowSkipException

from functools import lru_cache


@lru_cache(maxsize=32)
def get_snapshot_models_from_paths(
    applicative_path: str, intermediate_path: str
) -> tuple[str, ...]:
    """
    Get list of snapshot models from filesystem paths.
    Cached to avoid repeated os.listdir calls.
    Returns tuple for hashability.
    """
    snapshot_models = [
        filename[:-4]
        for path in [applicative_path, intermediate_path]
        for filename in os.listdir(path)
        if filename.endswith(".sql")
    ]
    return tuple(snapshot_models)


@lru_cache(maxsize=128)
def get_snapshot_configs_with_mtime(
    manifest_path: str,
    manifest_mtime: float,
    snapshot_names_tuple: tuple[str, ...],
) -> dict[str, dict[str, str]]:
    """
    Extract snapshot configs from manifest.
    Cache invalidates automatically when manifest changes.

    Args:
        manifest_path: Path to dbt target directory
        manifest_mtime: Manifest modification time (part of cache key)
        snapshot_names_tuple: Tuple of snapshot names to process

    Returns:
        Dict mapping snapshot_name to {table_alias, source_dataset}
    """
    # Use the cached load_manifest_with_mtime function
    manifest = load_manifest_with_mtime(manifest_path, manifest_mtime)
    snapshot_configs = {}

    for snapshot_name in snapshot_names_tuple:
        try:
            alias, dataset = get_node_alias_and_dataset(
                snapshot_name, manifest, node_type="snapshot"
            )
            snapshot_configs[snapshot_name] = {
                "table_alias": alias,
                "source_dataset": dataset,
            }
        except KeyError:
            import logging

            logging.warning(f"Snapshot {snapshot_name} not found in manifest")
            continue

    return snapshot_configs


def get_snapshot_configs_cached(
    manifest_path: str,
    snapshot_names: tuple[str, ...],
) -> dict[str, dict[str, str]]:
    """
    Get cached snapshot configs with automatic invalidation on manifest changes.

    Performance: With 3 DAGs, 8 processes:
    - Without caching: ~8,640 manifest loads/day
    - With mtime caching: ~3-8 loads/day (only when manifest changes)

    Args:
        manifest_path: Path to dbt target directory
        snapshot_names: Tuple of snapshot names to process

    Returns:
        Dict mapping snapshot_name to {table_alias, source_dataset}
    """
    manifest_file = os.path.join(manifest_path, "manifest.json")
    try:
        mtime = os.path.getmtime(manifest_file)
    except FileNotFoundError:
        return {}

    return get_snapshot_configs_with_mtime(manifest_path, mtime, snapshot_names)


def skip_if_data_exists(bucket, base_path, table, yyyymmdd):
    from google.cloud import storage
    from airflow.exceptions import AirflowSkipException

    client = storage.Client()
    prefix = f"{base_path}/{table}/{yyyymmdd}/"
    blobs = list(client.list_blobs(bucket, prefix=prefix))

    if any(b.name.endswith(".parquet") for b in blobs):
        raise AirflowSkipException("Data already exists, skipping export")


def extract_schema_to_gcs(
    project_id: str,
    dataset: str,
    table: str,
    bucket: str,
    base_path: str,
    yyyymmdd: str,
):
    bq = bigquery.Client(project=project_id)
    gcs = storage.Client()

    schema_path = f"{base_path}/{table}/{yyyymmdd}/schema.json"
    checksum_path = f"{base_path}/{table}/{yyyymmdd}/schema.sha256"

    bucket_obj = gcs.bucket(bucket)
    schema_blob = bucket_obj.blob(schema_path)
    checksum_blob = bucket_obj.blob(checksum_path)

    # Fetch schema from INFORMATION_SCHEMA
    query = f"""
        SELECT column_name, data_type
        FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """
    rows = list(bq.query(query).result())

    schema_doc = {
        "project": project_id,
        "dataset": dataset,
        "table": table,
        "columns": [{"name": r.column_name, "type": r.data_type} for r in rows],
    }

    schema_json = json.dumps(schema_doc, separators=(",", ":"), sort_keys=True)
    checksum = hashlib.sha256(schema_json.encode()).hexdigest()

    # If schema already exists → enforce immutability
    if schema_blob.exists():
        existing_checksum = checksum_blob.download_as_text()
        if existing_checksum != checksum:
            raise RuntimeError(
                f"Schema changed for {table} on {yyyymmdd} — refusing to overwrite"
            )
        # Same schema → idempotent replay
        raise AirflowSkipException("Schema already exists and matches")

    # First write
    schema_blob.upload_from_string(schema_json, content_type="application/json")
    checksum_blob.upload_from_string(checksum)


SNAPSHOTED_APPLICATIVE_PATH = f"{PATH_TO_DBT_PROJECT}/snapshots/raw_applicative"
SNAPSHOTED_INTERMEDIATE_PATH = f"{PATH_TO_DBT_PROJECT}/snapshots/intermediate"

# Cached filesystem scan
SNAPSHOTED_MODELS_TUPLE = get_snapshot_models_from_paths(
    SNAPSHOTED_APPLICATIVE_PATH, SNAPSHOTED_INTERMEDIATE_PATH
)
SNAPSHOTED_MODELS = list(SNAPSHOTED_MODELS_TUPLE)

# Cached schedule extraction (uses cached load_manifest internally)
MODELS_SCHEDULES = get_models_schedule_from_manifest(
    SNAPSHOTED_MODELS, PATH_TO_DBT_TARGET, resource_type="snapshot"
)

SNAPSHOT_CONFIGS = get_snapshot_configs_cached(
    PATH_TO_DBT_TARGET, SNAPSHOTED_MODELS_TUPLE
)

GCS_SNAPSHOT_BACKUP_FOLDER = "bigquery_snapshot_backup"
GCS_SNAPSHOT_BACKUP_BUCKET = DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME


def generate_export_query(alias, dataset, yyyymmdd, snapshot_at):
    return f"""
        EXPORT DATA OPTIONS(
            uri='gs://{GCS_SNAPSHOT_BACKUP_BUCKET}/{GCS_SNAPSHOT_BACKUP_FOLDER}/data/{alias}/{yyyymmdd}/*.parquet',
            format='PARQUET',
            compression='SNAPPY',
            overwrite=false
        ) AS
        SELECT
            TIMESTAMP('{snapshot_at}') AS backup_at,
            TO_JSON_STRING(t) AS row_values
        FROM `{GCP_PROJECT_ID}.{dataset}.{alias}` t
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
                "Skipping because table is weekly and monthly scheduled"
            )
    # check weekly
    elif "weekly" in schedules:
        if execution_date.weekday() != 0:  # Monday
            raise AirflowSkipException("Skipping because table is weekly scheduled")
    # check monthly
    elif "monthly" in schedules:
        if execution_date.day != 1:
            raise AirflowSkipException("Skipping because table is monthly scheduled")
    else:
        pass  # no schedule, default daily, always run


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
    for table_name, bq_config in SNAPSHOT_CONFIGS.items():
        alias = bq_config["table_alias"]
        dataset = bq_config["source_dataset"]

        # Use Airflow logical date (ds) for snapshot consistency
        # yyyymmdd for paths, snapshot_at for timestamp in schema and data
        yyyymmdd = "{{ ds_nodash }}"
        snapshot_at = "{{ ds }}"

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
            external_task_id=f"snapshots.{alias}",
        )

        schema_task = PythonOperator(
            task_id=f"extract_schema_{table_name}",
            python_callable=extract_schema_to_gcs,
            op_kwargs={
                "project_id": GCP_PROJECT_ID,
                "dataset": dataset,
                "table": alias,
                "bucket": GCS_SNAPSHOT_BACKUP_BUCKET,
                "base_path": GCS_SNAPSHOT_BACKUP_FOLDER,
                "yyyymmdd": yyyymmdd,
                "snapshot_at": snapshot_at,
            },
        )

        data_guard = PythonOperator(
            task_id=f"skip_if_data_exists_{table_name}",
            python_callable=skip_if_data_exists,
            op_kwargs={
                "bucket": GCS_SNAPSHOT_BACKUP_BUCKET,
                "base_path": f"{GCS_SNAPSHOT_BACKUP_FOLDER}/data",
                "table": alias,
                "yyyymmdd": yyyymmdd,
            },
        )

        transform_and_export = BigQueryInsertJobOperator(
            task_id=f"transform_and_export_{table_name}",
            configuration={
                "query": {
                    "query": generate_export_query(
                        alias, dataset, yyyymmdd, snapshot_at
                    ),
                    "useLegacySql": False,
                }
            },
            location=GCP_REGION,
            dag=dag,
        )

        # Task dependencies
        skip_task >> waiting_task >> schema_task >> data_guard >> transform_and_export

end = EmptyOperator(task_id="end", dag=dag, trigger_rule="none_failed_min_one_success")

start >> to_gcs >> end
