from airflow import DAG
from airflow.models import Param
from common.operators.gce import (
    StartGCEOperator,
    StopGCEOperator,
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
import datetime
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_TMP_DATASET,
)
from common.utils import get_airflow_schedule
from common.alerts import task_fail_slack_alert
from common import macros

DATASET_ID = f"export_{ENV_SHORT_NAME}"
GCE_INSTANCE = f"export-posthog-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/posthog"
DATE = "{{ ts_nodash }}"

DAG_CONFIG = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
    "STORAGE_PATH": f"gs://{DATA_GCS_BUCKET_NAME}/posthog_export_{ENV_SHORT_NAME}/export_posthog_{DATE}",
    "BASE_DIR": "data-gcp/jobs/etl_jobs/internal/export_posthog/",
}

TABLE_PARAMS = {
    "native": "posthog_native_event",
    "adage": "posthog_adage_log",
    "pro": "posthog_pro_event",
}

GCE_PARAMS = {
    "instance_name": f"export-posthog-data-{ENV_SHORT_NAME}",
    "instance_type": "n1-standard-8",
}


schedule_dict = {"prod": "0 8 * * *", "dev": "0 12 * * *", "stg": "0 10 * * *"}

for job_name, table_name in TABLE_PARAMS.items():
    with DAG(
        f"export_posthog_{job_name}_catchup",
        default_args={
            "start_date": datetime.datetime(2023, 9, 1),
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=5),
            "project_id": GCP_PROJECT_ID,
        },
        description="Export to analytics data posthog",
        schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
        catchup=True,
        start_date=datetime.datetime(2024, 3, 1),
        max_active_runs=1,
        dagrun_timeout=datetime.timedelta(minutes=1440),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        params={
            "branch": Param(
                default="production" if ENV_SHORT_NAME == "prod" else "master",
                type="string",
            ),
            "instance_type": Param(
                default=GCE_PARAMS["instance_type"],
                type="string",
            ),
            "instance_name": Param(
                default=GCE_PARAMS["instance_name"],
                type="string",
            ),
            "days": Param(
                default=0,
                type="integer",
            ),
        },
    ) as dag:
        table_config_name = f"export_{job_name}"
        table_id = f"{DATE}_{table_config_name}"
        params_instance = "{{ params.instance_name }}"
        instance_name = f"{job_name}-{params_instance}"
        storage_path = f"{DAG_CONFIG['STORAGE_PATH']}/{DATE}_{table_config_name}/"
        export_task = BigQueryExecuteQueryOperator(
            task_id=f"{table_config_name}",
            sql=f"""SELECT * FROM {DATASET_ID}.{table_name} """
            """WHERE event_date = DATE("{{ add_days(ds, params.days) }}")""",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{table_id}",
            dag=dag,
        )

        export_bq = BigQueryInsertJobOperator(
            task_id=f"{table_config_name}_to_bucket",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": table_id,
                    },
                    "compression": None,
                    "destinationUris": f"{DAG_CONFIG['STORAGE_PATH']}/{table_id}/data-*.parquet",
                    "destinationFormat": "PARQUET",
                }
            },
            dag=dag,
        )

        gce_instance_start = StartGCEOperator(
            task_id=f"{table_config_name}_gce_start_task",
            preemptible=False,
            instance_name=instance_name,
            instance_type="{{ params.instance_type }}",
            retries=2,
            labels={"job_type": "long_task"},
        )

        fetch_code = CloneRepositoryGCEOperator(
            task_id=f"{table_config_name}_fetch_code",
            instance_name=instance_name,
            python_version="3.10",
            command="{{ params.branch }}",
            retries=2,
        )

        install_dependencies = SSHGCEOperator(
            task_id=f"{table_config_name}_install_dependencies",
            instance_name=instance_name,
            base_dir=DAG_CONFIG["BASE_DIR"],
            command="pip install -r requirements.txt --user",
            dag=dag,
        )

        events_export = SSHGCEOperator(
            task_id=f"{table_config_name}_events_export",
            instance_name=instance_name,
            base_dir=DAG_CONFIG["BASE_DIR"],
            command="python main.py " f"--source-gs-path {storage_path}",
            dag=dag,
        )

        gce_instance_stop = StopGCEOperator(
            task_id=f"{table_config_name}_gce_stop_task", instance_name=instance_name
        )

        (
            export_task
            >> export_bq
            >> gce_instance_start
            >> fetch_code
            >> install_dependencies
            >> events_export
            >> gce_instance_stop
        )
