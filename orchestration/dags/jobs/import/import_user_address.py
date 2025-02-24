from datetime import datetime, timedelta

from common import macros
from common.alerts import on_failure_combined_callback
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    ENV_SHORT_NAME,
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
from airflow.operators.dummy_operator import DummyOperator

GCE_INSTANCE = f"import-user-address-bulk-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/api_gouv"
DAG_NAME = "import_user_address_bulk"

dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}


schedule_interval = "0 * * * *" if ENV_SHORT_NAME == "prod" else "30 2 * * *"

default_args = {
    "start_date": datetime(2021, 3, 30),
    "on_failure_callback": on_failure_combined_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Importing new data from addresses api every day.",
    schedule_interval=get_airflow_schedule(schedule_interval),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    template_searchpath=DAG_FOLDER,
    user_defined_macros=macros.default,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "source_dataset_id": f"int_api_gouv_{ENV_SHORT_NAME}",
        "source_table_name": "user_address_candidate_queue",
        "destination_dataset_id": BIGQUERY_RAW_DATASET,
        "destination_table_name": "user_address",
        "max_rows": 50_000,
        "chunk_size": 500,
    },
) as dag:
    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
    )

    addresses_to_gcs = SSHGCEOperator(
        task_id="user_address_to_bq",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="""python main.py \
            --source-dataset-id {{ params.source_dataset_id }} \
            --source-table-name {{ params.source_table_name }} \
            --destination-dataset-id {{ params.destination_dataset_id }} \
            --destination-table-name {{ params.destination_table_name }}
            --max-rows {{ params.max_rows }}
            --chunk-size {{ params.chunk_size }}
        """,
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> addresses_to_gcs
        >> gce_instance_stop
    ) >> end
