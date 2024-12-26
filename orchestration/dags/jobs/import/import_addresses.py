from datetime import datetime, timedelta

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

USER_LOCATIONS_SCHEMA = [
    {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_address", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_postal_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_department_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "city_code", "type": "STRING", "mode": "NULLABLE"},
    {"name": "api_adresse_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "code_epci", "type": "STRING", "mode": "NULLABLE"},
    {"name": "epci_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "qpv_communes", "type": "STRING", "mode": "NULLABLE"},
    {"name": "qpv_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "code_qpv", "type": "STRING", "mode": "NULLABLE"},
    {"name": "zrr", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date_updated", "type": "DATETIME", "mode": "NULLABLE"},
]

GCE_INSTANCE = f"import-addresses-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/addresses"
dag_config = {
    "GCP_PROJECT": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

USER_LOCATIONS_TABLE = "user_locations"
schedule_interval = (
    "0 2,4,6,8,12,16 * * *" if ENV_SHORT_NAME == "prod" else "30 2 * * *"
)

default_args = {
    "start_date": datetime(2021, 3, 30),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def branch_function(ti, **kwargs):
    xcom_value = ti.xcom_pull(task_ids="addresses_to_gcs", key="result")
    if "No new users !" not in xcom_value:
        return "import_addresses_to_bigquery"
    else:
        return "end"


with DAG(
    "import_addresses_v1",
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
    },
) as dag:
    start = DummyOperator(task_id="start")

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=BASE_PATH,
    )

    addresses_to_gcs = SSHGCEOperator(
        task_id="addresses_to_gcs",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )
    branch_op = BranchPythonOperator(
        task_id="checking_if_new_users",
        python_callable=branch_function,
        provide_context=True,
        do_xcom_push=False,
        dag=dag,
    )

    import_addresses_to_bigquery = GCSToBigQueryOperator(
        task_id="import_addresses_to_bigquery",
        bucket=DATA_GCS_BUCKET_NAME,
        source_objects=[
            "{{task_instance.xcom_pull(task_ids='addresses_to_gcs', key='result')}}"
        ],
        destination_project_dataset_table=f"{BIGQUERY_RAW_DATASET}.{USER_LOCATIONS_TABLE}",
        write_disposition="WRITE_APPEND",
        source_format="CSV",
        autodetect=False,
        schema_fields=USER_LOCATIONS_SCHEMA,
        skip_leading_rows=1,
        field_delimiter="|",
    )

    end = DummyOperator(task_id="end", trigger_rule="one_success")

    (
        start
        >> gce_instance_start
        >> fetch_install_code
        >> addresses_to_gcs
        >> gce_instance_stop
        >> branch_op
    )
    (branch_op >> import_addresses_to_bigquery >> end)
    branch_op >> end
