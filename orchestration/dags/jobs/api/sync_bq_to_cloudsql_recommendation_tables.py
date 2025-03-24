from datetime import datetime, timedelta

from common import macros
from common.alerts import on_failure_combined_callback
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
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "start_date": datetime(2025, 3, 10),
    "on_failure_callback": on_failure_combined_callback,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
}

DEFAULT_REGION = "europe-west1"
BASE_DIR = "data-gcp/jobs/etl_jobs/internal/export_recommendation"
DAG_ID = "sync_bq_to_cloudsql_recommendation_tables"

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

# List of tables to process - configuration is in config.py
TABLES_TO_PROCESS = list(TABLE_DEPENDENCIES.keys())

# List of all materialized views to refresh
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


with DAG(
    DAG_ID,
    default_args=default_args,
    description="Sync BigQuery tables to Cloud SQL for recommendation engine",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_ID]),
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
    },
) as dag:
    start = DummyOperator(task_id="start")

    # Wait for upstream tables
    with TaskGroup(group_id="wait_for_tables") as wait_for_tables:
        for table_name, dependency in TABLE_DEPENDENCIES.items():
            waiting_task = delayed_waiting_operator(
                dag=dag,
                external_dag_id=dependency["dag_id"],
                external_task_id=dependency["task_id"],
            )
            start >> waiting_task

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
                python main.py export-gcs \
                    --table-name {table_name} \
                    --bucket-path gs://{DATA_GCS_BUCKET_NAME}/export/recommendation_exports/{{{{ ds_nodash }}}} \
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
                python main.py import-to-gcloud \
                    --table-name {table_name} \
                    --bucket-path gs://{DATA_GCS_BUCKET_NAME}/export/recommendation_exports/{{{{ ds_nodash }}}} \
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
        bucket_name=DATA_GCS_BUCKET_NAME,
        prefix="export/recommendation_exports/{{ ds_nodash }}/",
        impersonation_chain=None,  # Add if needed for your setup
    )

    # Refresh all materialized views concurrently
    with TaskGroup("refresh_materialized_views", dag=dag) as refresh_views:
        for view in MATERIALIZED_VIEWS:
            refresh_command = f"""
                python main.py materialize-gcloud \
                    --view-name {view}
            """

            SSHGCEOperator(
                task_id=f"refresh_{view}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                command=refresh_command,
                dag=dag,
            )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    # Set dependencies
    (
        wait_for_tables
        >> gce_instance_start
        >> fetch_install_code
        >> export_tables
        >> import_tables
    )
    import_tables >> cleanup_gcs >> refresh_views >> gce_instance_stop
