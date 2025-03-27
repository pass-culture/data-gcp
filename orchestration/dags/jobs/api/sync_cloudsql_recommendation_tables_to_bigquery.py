import datetime
import os

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

DEFAULT_DAG_ARGS = {
    "start_date": datetime.datetime(2023, 1, 1),
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT_ID,
}

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")


INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-2",
    "prod": "n1-standard-4",
}[ENV_SHORT_NAME]


SCHEDULE_DICT = {
    "dev": "0 5 * * *",  # every day at 5:00 AM
    "stg": "0 5 * * *",  # every day at 5:00 AM
    "prod": "5 * * * *",  # every hour at 5 minutes past the hour
}[ENV_SHORT_NAME]

# Base directory for the export job
BASE_DIR = "data-gcp/jobs/etl_jobs/internal/sync_recommendation"
DAG_ID = "sync_cloudsql_recommendation_tables_to_bigquery"


NOW = datetime.datetime.now()

with DAG(
    DAG_ID,
    default_args=DEFAULT_DAG_ARGS,
    description="Import tables from recommendation CloudSQL to BigQuery hourly",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=45),
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
            default=f"cloudsql-to-bq-export-{ENV_SHORT_NAME}",
            type="string",
        ),
        "table_name": Param(
            default="past_offer_context",
            type="string",
        ),
        "bucket_path": Param(
            default=f"gs://{DATA_GCS_BUCKET_NAME}",
            type="string",
        ),
        "bucket_folder": Param(
            default=f"import/cloudsql_recommendation_tables/{NOW.strftime('%Y%m%d')}/",
            type="string",
        ),
        "execution_date": Param(
            default=NOW.strftime("%Y%m%d"),
            type="string",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start", dag=dag)

    # Start VM for processing
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"dag_name": DAG_ID},
        preemptible=False,
    )

    # Install code on VM
    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.12",
        base_dir=BASE_DIR,
        retries=2,
    )

    # Run the hourly export process
    export_data_to_gcs = SSHGCEOperator(
        task_id="export_data_to_gcs",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""
            python sql_to_bq.py cloudsql-to-gcs \
                --table-name {{ params.table_name }} \
                --bucket-path {{ params.bucket_path }}/{{ params.bucket_folder }} \
                --execution-date {{ params.execution_date }} \
                --end-time {{ ds }}
        """,
        dag=dag,
    )

    # Import data from GCS to BigQuery
    import_data_to_bigquery = SSHGCEOperator(
        task_id="import_data_to_bigquery",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""
            python sql_to_bq.py gcs-to-bq \
                --table-name {{ params.table_name }} \
                --bucket-path {{ params.bucket_path }}/{{ params.bucket_folder }} \
                --execution-date {{ params.execution_date }}
        """,
        dag=dag,
    )

    # Remove processed data from cloudSQL
    remove_processed_data = SSHGCEOperator(
        task_id="remove_processed_data",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""
            python sql_to_bq.py remove-cloudsql-data \
                --table-name {{ params.table_name }} \
        """,
    )

    # Delete the VM after processing
    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    # Cleanup GCS files after successful import
    cleanup_gcs = GCSDeleteObjectsOperator(
        task_id="cleanup_gcs_files",
        bucket_name="{{ params.bucket_path }}",
        prefix="{{ params.bucket_folder }}",
        impersonation_chain=None,
    )

    # Set up task dependencies
    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> export_data_to_gcs
        >> import_data_to_bigquery
        >> remove_processed_data
        >> gce_instance_stop
        >> end
    )
