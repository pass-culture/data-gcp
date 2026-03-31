import os
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
from pydantic import BaseModel, ConfigDict, Field

from jobs.crons import SCHEDULE_DICT
from jobs.ml.constants import IMPORT_ENDPOINT_MONITORING_SQL_PATH

# DAG Config
TIMESTAMP = "{{ ts_nodash }}"
DATE = "{{ ds_nodash }}"
DEFAULT_ARGS = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


class DagBaseConfig(BaseModel):
    model_config = ConfigDict(
        frozen=True, validate_default=True, strict=True, extra="forbid"
    )


class GCEConfig(DagBaseConfig):
    _INSTANCE_TYPES = {
        "dev": "n1-standard-2",
        "stg": "n1-standard-4",
        "prod": "n1-standard-4",
    }
    instance_name: str = f"recommendation-endpoint-monitoring-{ENV_SHORT_NAME}"
    instance_type: str = _INSTANCE_TYPES[ENV_SHORT_NAME]


class BigQueryConfig(DagBaseConfig):
    input_user_table: str = f"test_users_{DATE}"
    input_item_table: str = f"test_items_{DATE}"
    output_report_table: str = f"endpoint_monitoring_report_{DATE}"


# Alerting
class AlertingConfig(DagBaseConfig):
    metabase_url: str = "https://analytics.data.passculture.team/dashboard/986-test-dintregation-recommendation"
    endpoint_name: str = f"recommendation_user_retrieval_{ENV_SHORT_NAME}"


# Parameters for the recommendation endpoint monitoring tests
class EndpointMonitoringConfig(DagBaseConfig):
    endpoint_name: str = f"recommendation_user_retrieval_{ENV_SHORT_NAME}"
    number_of_ids: int = Field(default=100, gt=0)
    number_of_mock_ids: int = Field(default=100, gt=0)
    number_of_calls_per_user: int = Field(default=2, gt=0)


class DagConfig(DagBaseConfig):
    dag_id: str = "recommendation_endpoint_monitoring"
    base_dir: str = "data-gcp/jobs/ml_jobs/endpoint_monitoring"
    source_experiment_name: dict = {
        "dev": f"dummy_{ENV_SHORT_NAME}",
        "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    }
    gcs_path: str = f"endpoint_monitoring_{ENV_SHORT_NAME}/{TIMESTAMP}"
    storage_path: str = (
        f"gs://{ML_BUCKET_TEMP}/endpoint_monitoring_{ENV_SHORT_NAME}/{TIMESTAMP}"
    )
    gce: GCEConfig = GCEConfig()
    bigquery: BigQueryConfig = BigQueryConfig()
    endpoint_monitoring: EndpointMonitoringConfig = EndpointMonitoringConfig()
    alerting: AlertingConfig = AlertingConfig()


DAG_CONFIG = DagConfig()

with (
    DAG(
        dag_id=DAG_CONFIG.dag_id,
        default_args=DEFAULT_ARGS,
        description="Run recommendation endpoint monitoring",
        schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_CONFIG.dag_id]),
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
            "instance_name": Param(default=DAG_CONFIG.gce.instance_name, type="string"),
            "instance_type": Param(
                default=DAG_CONFIG.gce.instance_type,
                type="string",
            ),
            "experiment_name": Param(
                default=DAG_CONFIG.source_experiment_name[ENV_SHORT_NAME],
                type=["string", "null"],
            ),
            "endpoint_name": Param(
                default=DAG_CONFIG.endpoint_monitoring.endpoint_name,
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
                        "tableId": DAG_CONFIG.bigquery.input_user_table,
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
                        "tableId": DAG_CONFIG.bigquery.input_item_table,
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
                        "tableId": DAG_CONFIG.bigquery.input_user_table,
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        DAG_CONFIG.storage_path,
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
                        "tableId": DAG_CONFIG.bigquery.input_item_table,
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        DAG_CONFIG.storage_path,
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
        labels={"dag_name": DAG_CONFIG.dag_id, "env": ENV_SHORT_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=INSTANCE_NAME,
        branch="{{ params.branch }}",
        python_version="3.11",
        base_dir=DAG_CONFIG.base_dir,
        retries=2,
    )

    run_recommendation_monitoring_tests = SSHGCEOperator(
        task_id="run_recommendation_monitoring_tests",
        instance_name=INSTANCE_NAME,
        base_dir=DAG_CONFIG.base_dir,
        command="python run_recommendation_monitoring_tests.py "
        "--endpoint-name {{ params.endpoint_name }} "
        "--experiment-name {{ params.experiment_name }} "
        f"--storage-path {DAG_CONFIG.storage_path} "
        f"--number-of-ids {DAG_CONFIG.endpoint_monitoring.number_of_ids} "
        f"--number-of-mock-ids {DAG_CONFIG.endpoint_monitoring.number_of_mock_ids} "
        f"--number-of-calls-per-user {DAG_CONFIG.endpoint_monitoring.number_of_calls_per_user} ",
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
        source_objects=f"""{DAG_CONFIG.gcs_path}/endpoint_monitoring_reports.parquet""",
        destination_project_dataset_table=(
            f"{BIGQUERY_ANALYTICS_DATASET}.{DAG_CONFIG.bigquery.output_report_table}"
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
            endpoint_name=DAG_CONFIG.endpoint_monitoring.endpoint_name,
            metabase_url=DAG_CONFIG.alerting.metabase_url,
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
