from datetime import datetime, timedelta

from common import macros
from common.callback import on_failure_vm_callback
from common.config import DAG_FOLDER, DAG_TAGS, DATA_GCS_BUCKET_NAME, ENV_SHORT_NAME
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from jobs.crons import SCHEDULE_DICT

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "start_date": datetime(2025, 3, 10),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
}

DEFAULT_REGION = "europe-west1"
BASE_DIR = "data-gcp/jobs/etl_jobs/internal/sync_recommendation"
DAG_ID = "sync_bigquery_to_cloudsql_recommendation_tables"

# Table dependencies configuration
TABLE_DEPENDENCIES = {
    "enriched_user": {
        "dag_id": "dbt_run_dag",
        "task_id": "data_transformation.ml_reco__user_statistics",
    },
    "recommendable_offers_raw": {
        "dag_id": "dbt_run_dag",
        "task_id": "data_transformation.ml_reco__recommendable_offer",
    },
    "non_recommendable_items_data": {
        "dag_id": "dbt_run_dag",
        "task_id": "data_transformation.ml_reco__user_booked_item",
    },
    "iris_france": {
        "dag_id": "dbt_run_dag",
        "task_id": "data_transformation.int_seed__iris_france",
    },
}

TABLES_TO_PROCESS = list(TABLE_DEPENDENCIES.keys())

MATERIALIZED_VIEWS = [
    "enriched_user_mv",
    "item_ids_mv",
    "non_recommendable_items_mv",
    "iris_france_mv",
    "recommendable_offers_raw_mv",
]

INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-4",
    "prod": "n1-standard-4",
}[ENV_SHORT_NAME]


def get_schedule_interval(dag_id: str):
    schedule_interval = SCHEDULE_DICT.get(dag_id, {}).get(ENV_SHORT_NAME, None)
    return get_airflow_schedule(schedule_interval)


def choose_branch(**context):
    run_id = context["dag_run"].run_id
    if run_id.startswith("scheduled__"):
        return ["waiting_group.waiting_branch"]
    return ["shunt_manual"]


with DAG(
    DAG_ID,
    default_args=default_args,
    description="Sync BigQuery tables to Cloud SQL for recommendation engine",
    schedule_interval=get_schedule_interval(DAG_ID),
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
            default=DATA_GCS_BUCKET_NAME,
            type="string",
        ),
        "bucket_folder": Param(
            default="export/cloudsql_recommendation_tables",
            type="string",
        ),
    },
) as dag:
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        provide_context=True,
        dag=dag,
    )

    with TaskGroup(group_id="waiting_group") as waiting_group:
        wait = DummyOperator(task_id="waiting_branch", dag=dag)
        for table_name, dependency in TABLE_DEPENDENCIES.items():
            waiting_task = delayed_waiting_operator(
                dag=dag,
                external_dag_id=dependency["dag_id"],
                external_task_id=dependency["task_id"],
            )
            wait >> waiting_task

    shunt = DummyOperator(task_id="shunt_manual", dag=dag)
    join = DummyOperator(task_id="join", dag=dag, trigger_rule="none_failed")

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
    )

    # Refresh all materialized views sequentially
    with TaskGroup("refresh_materialized_views", dag=dag) as refresh_views:
        previous_task = None
        for view in MATERIALIZED_VIEWS:
            refresh_command = f"""
                python bq_to_sql.py materialize-cloudsql \
                    --view-name {view}
            """

            current_task = SSHGCEOperator(
                task_id=f"refresh_{view}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                command=refresh_command,
                dag=dag,
            )

            if previous_task:
                previous_task >> current_task
            previous_task = current_task

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    # Set dependencies
    (
        branching
        >> [shunt, waiting_group]
        >> join
        >> gce_instance_start
        >> fetch_install_code
        >> export_tables
        >> import_tables
        >> cleanup_gcs
        >> refresh_views
        >> gce_instance_stop
    )
