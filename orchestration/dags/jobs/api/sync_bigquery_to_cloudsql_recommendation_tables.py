import logging
from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.models import DagRun, Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
    ENV_SHORT_NAME,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import (
    delayed_waiting_operator,
    get_airflow_schedule,
    get_last_execution_date,
)

from jobs.crons import SCHEDULE_DICT

default_args = {
    "start_date": datetime(2025, 3, 10),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
}

DEFAULT_REGION = "europe-west1"
BASE_DIR = "data-gcp/jobs/etl_jobs/internal/sync_recommendation"
DAG_ID = "sync_bigquery_to_cloudsql_recommendation_tables"


# Mapping used both to list the tables to process and to derive, for each
# table, the name of the materialized view that depends on it.
TABLE_TO_VIEW = {
    "enriched_user": "enriched_user_mv",
    "recommendable_offers_raw": "recommendable_offers_raw_mv",
    "non_recommendable_items_data": "non_recommendable_items_mv",
    "iris_france": "iris_france_mv",
    "venue_h3_mapping": "venue_h3_mapping_mv",
    "similar_artist": "similar_artist_mv",
    "offer_metadata": "offer_metadata_mv",
    "artist": "artist_mv",
    "artist_top_by_category": "artist_top_by_category_mv",
}

# dbt models (run by dbt_run_dag) backing each exported BigQuery table.
# iris_france is a seed source, not refreshed by dbt_run_dag.
TABLE_TO_DBT_MODEL = {
    "enriched_user": "ml_reco__user_statistics",
    "recommendable_offers_raw": "ml_reco__recommendable_offer",
    "non_recommendable_items_data": "ml_reco__user_booked_item",
    "venue_h3_mapping": "ml_reco__venue_h3_mapping",
    "similar_artist": "exp_reco_cloudsql__similar_artist",
    "offer_metadata": "exp_reco_cloudsql__offer_metadata",
    "artist": "exp_reco_cloudsql__artist",
    "artist_top_by_category": "exp_reco_cloudsql__artist_top_by_category",
}

DBT_DAG_ID = "dbt_run_dag"
# Same window as delayed_waiting_operator's default (window_days=1).
DBT_RUN_WINDOW = timedelta(days=1)
# Sensors wait for any terminal state; tables whose dbt model did not succeed
# are then excluded from the sync by choose_tables_to_export.
DBT_TERMINAL_STATES = ["success", "failed", "upstream_failed", "skipped"]
DBT_NOT_REFRESHED_STATES = {"failed", "upstream_failed", "skipped"}

# The Cloud SQL import heavily slows down the recommendation API: it must run
# at night. If dbt is not ready by then, the sync fails and nothing is synced.
SYNC_START_DEADLINE_TZ = "Europe/Paris"
SYNC_START_DEADLINE_HOUR = 6

TABLES_TO_PROCESS = list(TABLE_TO_VIEW.keys())
MATERIALIZED_VIEWS = list(TABLE_TO_VIEW.values())
VIEW_TO_TABLE = {view: table for table, view in TABLE_TO_VIEW.items()}

INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-4",
    "prod": "n1-standard-4",
}[ENV_SHORT_NAME]


def get_schedule(dag_id: str):
    schedule = SCHEDULE_DICT.get(dag_id, {}).get(ENV_SHORT_NAME, None)
    return get_airflow_schedule(schedule)


def sync_start_deadline(context) -> datetime:
    """06:00 Europe/Paris on the day the scheduled run is triggered."""
    return (
        context["data_interval_end"]
        .in_timezone(SYNC_START_DEADLINE_TZ)
        .replace(hour=SYNC_START_DEADLINE_HOUR, minute=0, second=0, microsecond=0)
    )


def get_tables_with_failed_dbt_model(logical_date: datetime) -> set[str]:
    """Tables whose dbt model was not refreshed by the latest dbt_run_dag run."""
    dbt_logical_date = get_last_execution_date(
        DBT_DAG_ID,
        upper_date_limit=logical_date,
        lower_date_limit=logical_date - DBT_RUN_WINDOW,
    )
    if dbt_logical_date is None:
        logging.warning("No %s run found, no table excluded.", DBT_DAG_ID)
        return set()

    dbt_dag_run = DagRun.find(dag_id=DBT_DAG_ID, execution_date=dbt_logical_date)[0]
    failed_tables = set()
    for table, dbt_model in TABLE_TO_DBT_MODEL.items():
        ti = dbt_dag_run.get_task_instance(f"data_transformation.{dbt_model}")
        state = ti.state if ti else None
        if state in DBT_NOT_REFRESHED_STATES:
            logging.warning(
                "dbt model %s is in state %s: table %s will not be synced.",
                dbt_model,
                state,
                table,
            )
            failed_tables.add(table)
    return failed_tables


def _selected_tables(context) -> list[str]:
    return context["params"].get("tables") or TABLES_TO_PROCESS


def _tables_to_sync(context) -> list[str]:
    return context["ti"].xcom_pull(task_ids="branch_export_tables", key="tables")


def choose_tables_to_export(**context):
    failed_tables = get_tables_with_failed_dbt_model(context["logical_date"])
    tables = [t for t in _selected_tables(context) if t not in failed_tables]
    logging.info("Tables to sync: %s", tables)
    # Shared with the import/refresh steps so they all use the same list.
    context["ti"].xcom_push(key="tables", value=tables)
    return [f"export_tables.export_{table}" for table in tables]


def choose_tables_to_import(**context):
    return [f"import_tables.import_{table}" for table in _tables_to_sync(context)]


def is_view_selected(view: str, **context) -> bool:
    return VIEW_TO_TABLE[view] in _tables_to_sync(context)


with DAG(
    DAG_ID,
    default_args=default_args,
    description="Sync BigQuery tables to Cloud SQL for recommendation engine",
    schedule=get_schedule(DAG_ID),
    catchup=False,
    dagrun_timeout=timedelta(minutes=480),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=INSTANCE_TYPE,
            type="string",
        ),
        "instance_name": Param(
            default=f"recommendation-export-{ENV_SHORT_NAME}",
            type="string",
        ),
        "bucket_name": Param(
            default=DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
            type="string",
        ),
        "bucket_folder": Param(
            default="export/cloudsql_recommendation_tables",
            type="string",
        ),
        "tables": Param(
            default=TABLES_TO_PROCESS,
            type="array",
            examples=TABLES_TO_PROCESS,
            description=(
                "List of tables to sync. By default, all tables are synced. "
                f"Possible values: {TABLES_TO_PROCESS}."
            ),
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    # Wait for the dbt models feeding the exported tables to reach a terminal
    # state in today's dbt_run_dag run, so the sync does not export stale
    # data. Fails the whole sync if they are not ready by the deadline.
    with TaskGroup("wait_for_dbt", dag=dag) as wait_for_dbt:
        for dbt_model in TABLE_TO_DBT_MODEL.values():
            delayed_waiting_operator(
                dag,
                external_dag_id=DBT_DAG_ID,
                external_task_id=f"data_transformation.{dbt_model}",
                allowed_states=DBT_TERMINAL_STATES,
                failed_states=[],
                skip_manually_triggered=True,
                deadline_fn=sync_start_deadline,
                retries=0,
            )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "long_task", "dag_name": DAG_ID},
        preemptible=False,
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.12",
        base_dir=BASE_DIR,
        retries=2,
    )

    # Branch to only run export/import/refresh tasks for the selected tables
    branch_export_tables = BranchPythonOperator(
        task_id="branch_export_tables",
        python_callable=choose_tables_to_export,
    )

    # Export tasks group
    with TaskGroup("export_tables", dag=dag) as export_tables:
        for table_name in TABLES_TO_PROCESS:
            export_command = f"""
                python bq_to_sql.py bq-to-gcs \
                    --table-name {table_name} \
                    --bucket-path gs://{{{{ params.bucket_name }}}}/{{{{ params.bucket_folder }}}}/{{{{ ts_nodash }}}} \
                    --date {{{{ ds_nodash }}}}
            """

            SSHGCEOperator(
                task_id=f"export_{table_name}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                command=export_command,
                dag=dag,
            )

    branch_import_tables = BranchPythonOperator(
        task_id="branch_import_tables",
        python_callable=choose_tables_to_import,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Import tasks group
    with TaskGroup("import_tables", dag=dag) as import_tables:
        for table_name in TABLES_TO_PROCESS:
            import_command = f"""
                python bq_to_sql.py gcs-to-cloudsql \
                    --table-name {table_name} \
                    --bucket-path gs://{{{{ params.bucket_name }}}}/{{{{ params.bucket_folder }}}}/{{{{ ts_nodash }}}} \
                    --date {{{{ ds_nodash }}}}
            """

            SSHGCEOperator(
                task_id=f"import_{table_name}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                command=import_command,
                dag=dag,
            )

    # Cleanup GCS files after successful import
    cleanup_gcs = GCSDeleteObjectsOperator(
        task_id="cleanup_gcs_files",
        bucket_name="{{ params.bucket_name }}",
        prefix="{{ params.bucket_folder }}",
        impersonation_chain=None,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Refresh all materialized views sequentially
    with TaskGroup("refresh_materialized_views", dag=dag) as refresh_views:
        previous_task = None
        for view in MATERIALIZED_VIEWS:
            # One ShortCircuitOperator per view, inserted directly in the
            # sequential chain: a single upstream task per decision point,
            # no shared branch with multiple explicit edges. This avoids
            # the ambiguous skip propagation that previously caused ALL
            # views to run regardless of the selected tables.
            check_view = ShortCircuitOperator(
                task_id=f"check_{view}",
                python_callable=partial(is_view_selected, view),
                ignore_downstream_trigger_rules=False,
            )

            refresh_command = f"""
                python bq_to_sql.py materialize-cloudsql \
                    --view-name {view}
            """

            refresh_materialized_view = SSHGCEOperator(
                task_id=f"refresh_{view}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                command=refresh_command,
                dag=dag,
            )

            check_view >> refresh_materialized_view

            # Add a 30 sec delay before each view refresh (except the first).
            if previous_task:
                wait_between_refreshes = BashOperator(
                    task_id=f"wait_before_{view}_refresh",
                    bash_command="sleep 30",
                    trigger_rule=TriggerRule.NONE_FAILED,
                )
                previous_task >> wait_between_refreshes >> check_view
            previous_task = refresh_materialized_view

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Set dependencies
    (
        start
        >> wait_for_dbt
        >> gce_instance_start
        >> fetch_install_code
        >> branch_export_tables
        >> export_tables
        >> branch_import_tables
        >> import_tables
        >> cleanup_gcs
        >> refresh_views
    )
    refresh_views >> gce_instance_stop
