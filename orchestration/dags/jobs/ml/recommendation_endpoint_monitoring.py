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

from jobs.crons import SCHEDULE_DICT
from jobs.ml.constants import IMPORT_ENDPOINT_MONITORING_SQL_PATH


def create_ssh_task(task_id, command, instance_name, base_dir):
    """Helper to create an SSHGCEOperator with minimal boilerplate."""
    return SSHGCEOperator(
        task_id=task_id,
        instance_name=instance_name,
        base_dir=base_dir,
        environment=DAG_CONFIG["ENVIROMENT"],
        command=command,
    )


DATE = "{{ ts_nodash }}"
DAG_CONFIG = {
    "ID": "endpoint_monitoring",
    "ENVIROMENT": {
        "GCP_PROJECT": GCP_PROJECT_ID,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
    },
    "BIGQUERY": {
        "INPUT_USER_TABLE": "test_users",
        "INPUT_ITEM_TABLE": "test_items",
        "OUTPUT_REPORT_TABLE": "endpoint_monitoring_report",
    },
    "STORAGE_PATH": f"gs://{ML_BUCKET_TEMP}/endpoint_monitoring_{ENV_SHORT_NAME}/{DATE}",
    "GCS_PATH": f"endpoint_monitoring_{ENV_SHORT_NAME}/{DATE}",
    "BASE_DIR": "data-gcp/jobs/ml_jobs/endpoint_monitoring",
    "source_experiment_name": {
        "dev": f"dummy_{ENV_SHORT_NAME}",
        "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    },
    "endpoint_name": f"recommendation_user_retrieval_{ENV_SHORT_NAME}",
    "number_of_ids": 100,
    "number_of_mock_ids": 100,
    "number_of_calls_per_user": 2,
}

# Environment variables to export before running commands
default_args = {
    "start_date": datetime(2022, 11, 30),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
gce_params = {
    "instance_name": f"recommendation-endpoint-monitoring-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-4",
        "prod": "n1-standard-4",
    },
}

with (
    DAG(
        dag_id=DAG_CONFIG["ID"],
        default_args=default_args,
        description="Run recommendation endpoint monitoring",
        schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_CONFIG["ID"]]),
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
            "instance_name": Param(default=gce_params["instance_name"], type="string"),
            "instance_type": Param(
                default=gce_params["instance_type"][ENV_SHORT_NAME],
                type="string",
            ),
            "experiment_name": Param(
                default=DAG_CONFIG["source_experiment_name"][ENV_SHORT_NAME],
                type=["string", "null"],
            ),
            "endpoint_name": Param(
                default=DAG_CONFIG["endpoint_name"],
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
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_USER_TABLE"],
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
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_ITEM_TABLE"],
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
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_USER_TABLE"],
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        DAG_CONFIG["STORAGE_PATH"],
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
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_ITEM_TABLE"],
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        DAG_CONFIG["STORAGE_PATH"],
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
        labels={"job_type": "ml", "dag_name": DAG_CONFIG["ID"]},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=INSTANCE_NAME,
        branch="{{ params.branch }}",
        python_version="'3.10'",
        base_dir=DAG_CONFIG["BASE_DIR"],
        retries=2,
    )

    run_endpoint_monitoring_tests = create_ssh_task(
        task_id="run_endpoint_monitoring_tests",
        command="python main.py "
        "--endpoint-name {{ params.endpoint_name }} "
        "--experiment-name {{ params.experiment_name }} "
        f"--storage-path {DAG_CONFIG['STORAGE_PATH']} "
        f"--number-of-ids {DAG_CONFIG['number_of_ids']} "
        f"--number-of-mock-ids {DAG_CONFIG['number_of_mock_ids']} "
        f"--number-of-calls-per-user {DAG_CONFIG['number_of_calls_per_user']} ",
        instance_name=INSTANCE_NAME,
        base_dir=DAG_CONFIG["BASE_DIR"],
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
        source_objects=f"""{DAG_CONFIG['GCS_PATH']}/endpoint_monitoring_reports.parquet""",
        destination_project_dataset_table=(
            f"{BIGQUERY_ANALYTICS_DATASET}.{DAG_CONFIG['BIGQUERY']['OUTPUT_REPORT_TABLE']}"
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
            endpoint_name=DAG_CONFIG["endpoint_name"],
            metabase_url="https://analytics.data.passculture.team/dashboard/986-test-dintregation-recommendation",
            env_short_name=ENV_SHORT_NAME,
        ),
    )
    (
        start
        >> import_data_group
        >> export_data_group
        >> gce_instance_start
        >> fetch_install_code
        >> run_endpoint_monitoring_tests
        >> gce_instance_stop
        >> load_endpoint_monitoring_report_to_bq
        >> send_slack_notif_success
    )
