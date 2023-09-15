import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from common.alerts import task_fail_slack_alert

from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.utils import getting_service_account_token, get_airflow_schedule

# FUNCTION_NAME = f"typeform_adage_reference_request_{ENV_SHORT_NAME}"

GCE_INSTANCE = f"import-typeform-adage-reference-request-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/typeform-adage-reference-request"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 1,
    "project_id": GCP_PROJECT_ID,
}


with DAG(
    "import_typeform_adage_reference_request",
    default_args=default_dag_args,
    description="Import Typeform Adage Reference Request from API",
    on_failure_callback=None,
    schedule_interval=get_airflow_schedule("0 1 * * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        )
    },
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE, task_id="gce_start_task"
    )

    fetch_code = CloneRepositoryGCEOperator(
        task_id="fetch_code",
        instance_name=GCE_INSTANCE,
        command="{{ params.branch }}",
        python_version="3.8",
    )

    install_dependencies = SSHGCEOperator(
        task_id="install_dependencies",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        command="pip install -r requirements.txt --user",
        dag=dag,
        retries=2,
    )

    typeform_adage_reference_request_to_bq = SSHGCEOperator(
        task_id="typeform_adage_reference_request_to_bq",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="python main.py ",
        do_xcom_push=True,
    )

    gce_instance_stop = StopGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    create_analytics_table = BigQueryInsertJobOperator(
        dag=dag,
        task_id="typeform_adage_reference_request_to_analytics",
        configuration={
            "query": {
                "query": f"SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.typeform_adage_reference_request_sheet`",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ANALYTICS_DATASET,
                    "tableId": "typeform_adage_reference_request",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    end = DummyOperator(task_id="end", dag=dag)

    (
        start
        >> gce_instance_start
        >> fetch_code
        >> install_dependencies
        >> typeform_adage_reference_request_to_bq
        >> gce_instance_stop
        >> create_analytics_table
        >> end
    )
