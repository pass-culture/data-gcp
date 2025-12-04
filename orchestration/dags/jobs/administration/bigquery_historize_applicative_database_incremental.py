import datetime
import os
from typing import Dict, Any
from common.callback import on_failure_base_callback
from common.config import (
    BIGQUERY_RAW_APPLICATIVE_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_TARGET,
    PATH_TO_DBT_PROJECT,
    GCP_REGION,
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
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException


# ═══════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════

SNAPSHOTED_APPLICATIVE_PATH = f"{PATH_TO_DBT_PROJECT}/snapshots/raw_applicative"
SNAPSHOTED_MODELS = [
    filename[:-4]
    for filename in os.listdir(SNAPSHOTED_APPLICATIVE_PATH)
    if filename.endswith(".sql")
]
MODELS_SCHEDULES = get_models_schedule_from_manifest(
    SNAPSHOTED_MODELS, PATH_TO_DBT_TARGET
)

SNAPSHOT_TABLES = get_tables_config_dict(
    PATH=SNAPSHOTED_APPLICATIVE_PATH,
    BQ_DATASET=BIGQUERY_RAW_APPLICATIVE_DATASET,
    is_source=True,
    dbt_alias=True,
)

if "applicative_database__titelive_products" in SNAPSHOT_TABLES:
    SNAPSHOT_TABLES["applicative_database__titelive_products"]["json_columns"] = [
        "json_raw"
    ]

HISTORICAL_BACKUP_PATH = "historization_incremental/applicative"

# ═══════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════


def skip_if_not_scheduled(**context):
    """
    Skip the task based on the dbt manifest data for this table.
    Expects 'table_config' in params with optional 'tags' or 'schedule'.
    """
    schedules = context["params"].get("schedule_config", {})
    ds = context["ds"]
    execution_date = datetime.datetime.strptime(ds, "%Y-%m-%d")

    if "weekly" in schedules and "monthly" in schedules:
        if execution_date.weekday() != 0 and execution_date.day != 1:
            raise AirflowSkipException(
                f"Skipping because table is weekly and monthly scheduled"
            )
    elif "weekly" in schedules:
        if execution_date.weekday() != 0:
            raise AirflowSkipException(f"Skipping because table is weekly scheduled")
    elif "monthly" in schedules:
        if execution_date.day != 1:
            raise AirflowSkipException(f"Skipping because table is monthly scheduled")


def check_if_first_backup(
    table_name: str, task_group: str | None, alias: str, **context
) -> str:
    """
    Check if this is the first backup for a table by looking for existing partitions in GCS.

    Returns:
        - 'export_initial_state_{table_name}' if no backups exist (first run)
        - 'export_delta_{table_name}' if backups exist (normal operation)
    """
    gcs_hook = GCSHook()
    backup_path = f"{HISTORICAL_BACKUP_PATH}/{alias}"
    prefix = f"{backup_path}/dt="

    try:
        # List all partitions for this table
        blobs = list(
            gcs_hook.list(
                bucket_name=DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME,
                prefix=prefix,
                delimiter="/",
            )
        )

        if not blobs:
            print(
                f"No existing backups found for {alias}. Running INITIAL STATE export."
            )
            return (
                f"{task_group}.export_initial_state_{table_name}"
                if task_group
                else f"export_initial_state_{table_name}"
            )
        else:
            print(
                f"Found {len(blobs)} existing backup partitions for {alias}. Running DELTA export."
            )
            return (
                f"{task_group}.export_delta_{table_name}"
                if task_group
                else f"export_delta_{table_name}"
            )

    except Exception as e:
        print(f"Error checking GCS for {alias}: {e}")
        # Default to initial export if we can't check
        return f"export_initial_state_{table_name}"


def get_table_columns(bq_config: Dict[str, Any]) -> str:
    """
    Generate column selection with CAST for JSON columns to prevent Parquet JSON detection.

    Args:
        bq_config: Table configuration dictionary

    Returns:
        Column selection string (either "* EXCEPT(...)" or just "*")
    """
    json_columns = bq_config.get("json_columns", [])

    if not json_columns:
        return "*"

    # Build EXCEPT clause and CAST statements
    except_cols = ", ".join(json_columns)
    cast_statements = ", ".join(
        [f"CAST({col} AS STRING) as {col}" for col in json_columns]
    )

    return f"* EXCEPT({except_cols}), {cast_statements}"


# ═══════════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════════

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 2,
    "on_failure_callback": on_failure_base_callback,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}

dag_id = "bigquery_historize_applicative_database_incremental"
dag = DAG(
    dag_id,
    default_args=default_dag_args,
    dagrun_timeout=datetime.timedelta(minutes=480),
    description="Incremental backup of dbt snapshots to GCS with automatic first-state handling",
    schedule_interval=get_airflow_schedule(
        SCHEDULE_DICT.get(dag_id, {}).get(ENV_SHORT_NAME, "0 3 * * *")
    ),
    catchup=False,
    tags=[DAG_TAGS.DE.value],
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag, trigger_rule="none_failed")


with TaskGroup(group_id="snapshot_backups", dag=dag) as backup_group:
    for table_name, bq_config in SNAPSHOT_TABLES.items():
        alias = bq_config["table_alias"]
        dataset = bq_config["source_dataset"]
        # ───────────────────────────────────────────────────────
        # 1. Check schedule (skip if not scheduled for today)
        # ───────────────────────────────────────────────────────
        skip_task = PythonOperator(
            task_id=f"skip_check_{table_name}",
            python_callable=skip_if_not_scheduled,
            provide_context=True,
            params={**bq_config, **(MODELS_SCHEDULES.get(table_name, {}))},
            dag=dag,
        )
        # ───────────────────────────────────────────────────────
        # 2. Wait for dbt snapshot to complete
        # ───────────────────────────────────────────────────────
        waiting_task = delayed_waiting_operator(
            dag,
            external_dag_id="dbt_run_dag",
            external_task_id=f"snapshots.{table_name}",
        )
        # ───────────────────────────────────────────────────────
        # 3. Check if this is first backup (branch decision)
        # ───────────────────────────────────────────────────────
        check_first_backup_task = BranchPythonOperator(
            task_id=f"check_first_backup_{table_name}",
            python_callable=check_if_first_backup,
            op_kwargs={
                "table_name": table_name,
                "alias": alias,
                "task_group": "snapshot_backups",
            },
            dag=dag,
        )

        # ───────────────────────────────────────────────────────
        # 4a. BRANCH 1: Export Initial State (first run only)
        # ───────────────────────────────────────────────────────
        export_initial_state = BigQueryInsertJobOperator(
            task_id=f"export_initial_state_{table_name}",
            configuration={
                "query": {
                    "query": f"""
                        EXPORT DATA OPTIONS(
                            uri='gs://{DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME}/{HISTORICAL_BACKUP_PATH}/{alias}/dt={{{{ ds_nodash }}}}/*.parquet',
                            format='PARQUET',
                            overwrite=true,
                            compression='ZSTD'
                        ) AS
                        SELECT
                            'INITIAL' as operation_type,
                            {get_table_columns(bq_config)}
                        FROM `{GCP_PROJECT_ID}.{dataset}.applicative_database_{alias}`
                    """,
                    "useLegacySql": False,
                }
            },
            location=GCP_REGION,
            dag=dag,
        )
        # ───────────────────────────────────────────────────────
        # 4b. BRANCH 2: Export Daily Delta (normal operation)
        # ───────────────────────────────────────────────────────

        export_delta = BigQueryInsertJobOperator(
            task_id=f"export_delta_{table_name}",
            configuration={
                "query": {
                    "query": f"""
                        EXPORT DATA OPTIONS(
                            uri='gs://{DE_BIGQUERY_DATA_ARCHIVE_BUCKET_NAME}/{HISTORICAL_BACKUP_PATH}/{alias}/dt={{{{ ds_nodash }}}}/*.parquet',
                            format='PARQUET',
                            overwrite=true,
                            compression='ZSTD'
                        ) AS
                        -- Records that became valid on this date (INSERTS/UPDATES)
                        SELECT
                            'INSERT' as operation_type,
                            {get_table_columns(bq_config)}
                        FROM `{GCP_PROJECT_ID}.{dataset}.applicative_database_{alias}`
                        WHERE DATE(dbt_valid_from) = '{{{{ ds }}}}'

                        UNION ALL

                        -- Records that became invalid on this date (DELETES/EXPIRATIONS)
                        SELECT
                            'DELETE' as operation_type,
                            {get_table_columns(bq_config)}
                        FROM `{GCP_PROJECT_ID}.{dataset}.{alias}`
                        WHERE DATE(dbt_valid_to) = '{{{{ ds }}}}'
                    """,
                    "useLegacySql": False,
                }
            },
            location=GCP_REGION,
            dag=dag,
        )

        (
            skip_task
            >> waiting_task
            >> check_first_backup_task
            >> [export_initial_state, export_delta]
            >> end
        )


start >> backup_group >> end
