import os
from dataclasses import dataclass
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.alerts import SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN
from common.alerts.endpoint_monitoring import (
    create_recommendation_endpoint_monitoring_slack_block,
)
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    ML_BUCKET_TEMP,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.operators.slack import SendSlackMessageOperator
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT
from jobs.ml.constants import IMPORT_ENDPOINT_MONITORING_SQL_PATH

# DAG Config
DAG_ID = "recommendation_endpoint_monitoring"
TIMESTAMP = "{{ ts_nodash }}"
DATE = "{{ ds_nodash }}"
BASE_DIR = "data-gcp/jobs/ml_jobs/endpoint_monitoring"
SOURCE_EXPERIMENT_NAME = {
    "dev": f"dummy_{ENV_SHORT_NAME}",
    "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
}
DEFAULT_ARGS = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


# GCP
GCS_PATH = f"endpoint_monitoring_{ENV_SHORT_NAME}/{TIMESTAMP}"
STORAGE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_PATH}"


@dataclass(frozen=True)
class GCEConfig:
    instance_name: str = f"recommendation-endpoint-monitoring-{ENV_SHORT_NAME}"
    instance_type: str = {
        "dev": "n1-standard-2",
        "stg": "n1-standard-4",
        "prod": "n1-standard-4",
    }[ENV_SHORT_NAME]


@dataclass(frozen=True)
class BigQueryConfig:
    input_user_table: str = f"test_users_{DATE}"
    input_item_table: str = f"test_items_{DATE}"
    output_report_table: str = f"endpoint_monitoring_report_{DATE}"


# Alerting
@dataclass(frozen=True)
class AlertingConfig:
    metabase_url = "https://analytics.data.passculture.team/dashboard/986-test-dintregation-recommendation"
    endpoint_name = f"recommendation_user_retrieval_{ENV_SHORT_NAME}"


# Parameters for the recommendation endpoint monitoring tests
@dataclass(frozen=True)
class EndpointMonitoringConfig:
    endpoint_name: str = f"recommendation_user_retrieval_{ENV_SHORT_NAME}"
    number_of_ids: int = 100
    number_of_mock_ids: int = 100
    number_of_calls_per_user: int = 2


GCE_CONFIG = GCEConfig()
BQ_CONFIG = BigQueryConfig()
ENDPOINT_MONITORING_CONFIG = EndpointMonitoringConfig()
ALERTING_CONFIG = AlertingConfig()

with (
    DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        description="Run recommendation endpoint monitoring",
        schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_ID]),
        catchup=False,
        dagrun_timeout=timedelta(minutes=60),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        render_template_as_native_obj=True,  # be careful using this because "3.10" is rendered as 3.1 if not double escaped
        doc_md="This is a DAG to run recommendation endpoint monitoring tests.",
        tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
        params={
            "branch": Param(
                default="production" if ENV_SHORT_NAME == "prod" else "master",
                type="string",
            ),
            "instance_name": Param(default=GCEConfig.instance_name, type="string"),
            "instance_type": Param(
                default=GCEConfig.instance_type,
                type="string",
            ),
            "experiment_name": Param(
                default=SOURCE_EXPERIMENT_NAME[ENV_SHORT_NAME],
                type=["string", "null"],
            ),
            "endpoint_name": Param(
                default=ENDPOINT_MONITORING_CONFIG.endpoint_name,
                type=["string", "null"],
            ),
        },
    ) as dag
):
    INSTANCE_NAME = "{{ params.instance_name }}"
    start = DummyOperator(task_id="start", dag=dag)
    with TaskGroup(
        "import_data", tooltip="Import data from SQL to BQ"
    ) as import_data_group:
        import_user_data = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_test_users",
            configuration={
                "query": {
                    "query": (
                        IMPORT_ENDPOINT_MONITORING_SQL_PATH / "user_data.sql"
                    ).as_posix(),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": BQ_CONFIG.input_user_table,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

        import_item_data = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_test_items",
            configuration={
                "query": {
                    "query": (
                        IMPORT_ENDPOINT_MONITORING_SQL_PATH / "item_data.sql"
                    ).as_posix(),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": BQ_CONFIG.input_item_table,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

    with TaskGroup(
        "export_data", tooltip="Export data from BQ to GCS"
    ) as export_data_group:
        export_user_data_to_bq = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="export_user_data_to_bq",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": BQ_CONFIG.input_user_table,
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        STORAGE_PATH,
                        "user_data.parquet",
                    ),
                }
            },
        )

        export_item_data_to_bq = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="export_item_data_to_bq",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": BQ_CONFIG.input_item_table,
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        STORAGE_PATH,
                        "item_data.parquet",
                    ),
                }
            },
        )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name=INSTANCE_NAME,
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"dag_name": DAG_ID},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=INSTANCE_NAME,
        branch="{{ params.branch }}",
        python_version="3.11",
        base_dir=BASE_DIR,
        retries=2,
    )

    run_recommendation_monitoring_tests = SSHGCEOperator(
        task_id="run_recommendation_monitoring_tests",
        instance_name=INSTANCE_NAME,
        base_dir=BASE_DIR,
        command="python run_recommendation_monitoring_tests.py "
        "--endpoint-name {{ params.endpoint_name }} "
        "--experiment-name {{ params.experiment_name }} "
        f"--storage-path {STORAGE_PATH} "
        f"--number-of-ids {EndpointMonitoringConfig.number_of_ids} "
        f"--number-of-mock-ids {EndpointMonitoringConfig.number_of_mock_ids} "
        f"--number-of-calls-per-user {EndpointMonitoringConfig.number_of_calls_per_user} ",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name=INSTANCE_NAME,
        trigger_rule="none_failed",
    )
    load_endpoint_monitoring_report_to_bq = GCSToBigQueryOperator(
        project_id=GCP_PROJECT_ID,
        task_id="load_endpoint_monitoring_report_to_bq",
        bucket=ML_BUCKET_TEMP,
        source_objects=f"""{GCS_PATH}/endpoint_monitoring_reports.parquet""",
        destination_project_dataset_table=(
            f"{BIGQUERY_ANALYTICS_DATASET}.{BQ_CONFIG.output_report_table}"
        ),
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    send_slack_notif_success = SendSlackMessageOperator(
        task_id="send_slack_notif_success",
        webhook_token=SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN,
        trigger_rule="none_failed",
        block=create_recommendation_endpoint_monitoring_slack_block(
            endpoint_name=ALERTING_CONFIG.endpoint_name,
            metabase_url=ALERTING_CONFIG.metabase_url,
            env_short_name=ENV_SHORT_NAME,
        ),
    )
    (
        start
        >> import_data_group
        >> export_data_group
        >> gce_instance_start
        >> fetch_install_code
        >> run_recommendation_monitoring_tests
        >> gce_instance_stop
        >> load_endpoint_monitoring_report_to_bq
        >> send_slack_notif_success
    )
