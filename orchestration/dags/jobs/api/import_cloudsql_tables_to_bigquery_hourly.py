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

DEFAULT_DAG_ARGS = {
    "start_date": datetime.datetime(2023, 1, 1),
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
    "project_id": GCP_PROJECT_ID,
}

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# Instance type based on environment
INSTANCE_TYPE = {
    "dev": "n1-standard-2",
    "stg": "n1-standard-2",
    "prod": "n1-standard-4",
}[ENV_SHORT_NAME]

# Base directory for the export job
BASE_DIR = "data-gcp/jobs/etl_jobs/internal/export_recommendation"


with DAG(
    "import_cloudsql_tables_to_bigquery_hourly",
    default_args=DEFAULT_DAG_ARGS,
    description="Import tables from recommendation CloudSQL to BigQuery hourly",
    schedule_interval=get_airflow_schedule(
        "5 * * * *"
    ),  # Run at 5 minutes past every hour
    catchup=False,
    dagrun_timeout=datetime.timedelta(
        minutes=45
    ),  # Ensure it finishes before the next hour
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
            default=f"cloudsql-export-{ENV_SHORT_NAME}",
            type="string",
        ),
        "table_name": Param(
            default="past_offer_context",
            type="string",
        ),
        "recover_missed": Param(
            default=True,
            type="boolean",
            description="Whether to check for and recover missed data from failed jobs",
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
        labels={
            "job_type": "cloudsql_export",
            "dag_name": "export_cloudsql_tables_to_bigquery_hourly",
        },
        preemptible=False,
    )

    # Install code on VM
    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_DIR,
        retries=2,
    )

    # Run the hourly export process
    run_hourly_export = SSHGCEOperator(
        task_id="run_hourly_export",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="""
            python main.py hourly_export_process \
                --table-name {{ params.table_name }} \
                --bucket-path gs://{bucket_name}/export/cloudsql_hourly \
                --date {{ ds_nodash }} \
                --hour {{ execution_date.hour }} \
                --recover-missed {{ params.recover_missed }}
        """.format(bucket_name=DATA_GCS_BUCKET_NAME),
        dag=dag,
    )

    # Delete the VM after processing
    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
    )

    # Set up task dependencies
    end = EmptyOperator(task_id="end", dag=dag)

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> run_hourly_export
        >> gce_instance_stop
        >> end
    )
