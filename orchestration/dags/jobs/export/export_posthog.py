import datetime

from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
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
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

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

CATCHUP_PARAMS = {
    "native": datetime.datetime(2024, 4, 25),
    "adage": datetime.datetime(2024, 1, 1),
    "pro": datetime.datetime(2024, 3, 1),
}

GCE_PARAMS = {
    "instance_name": f"export-posthog-data-{ENV_SHORT_NAME}",
    "instance_type": "n1-standard-8",
}


schedule_dict = {"prod": "0 8 * * *", "dev": None, "stg": None}

for job_name, table_name in TABLE_PARAMS.items():
    DAG_NAME = f"export_posthog_{job_name}"
    with DAG(
        DAG_NAME,
        default_args={
            "start_date": datetime.datetime(2023, 9, 1),
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=5),
            "project_id": GCP_PROJECT_ID,
            "on_failure_callback": on_failure_vm_callback,
            "on_skipped_callback": on_failure_vm_callback,
        },
        description="Export to analytics data posthog",
        schedule_interval=get_airflow_schedule(schedule_dict[ENV_SHORT_NAME]),
        catchup=False,
        start_date=CATCHUP_PARAMS[job_name],
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
        tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
    ) as dag:
        table_config_name = f"export_{job_name}"
        table_id = f"{DATE}_{table_config_name}"
        params_instance = "{{ params.instance_name }}"
        instance_name = f"{job_name}-{params_instance}"
        storage_path = f"{DAG_CONFIG['STORAGE_PATH']}/{DATE}_{table_config_name}/"
        export_task = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id=f"{table_config_name}",
            configuration={
                "query": {
                    "query": f"""SELECT * FROM {DATASET_ID}.{table_name} """
                    """WHERE event_date = DATE("{{ add_days(ds, params.days) }}")""",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": table_id,
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
            dag=dag,
        )

        export_bq = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
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
            labels={"job_type": "long_task", "dag_name": DAG_NAME},
        )
        fetch_install_code = InstallDependenciesOperator(
            task_id=f"{table_config_name}_fetch_install_code",
            instance_name=instance_name,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=DAG_CONFIG["BASE_DIR"],
            dag=dag,
            retries=2,
        )

        events_export = SSHGCEOperator(
            task_id=f"{table_config_name}_events_export",
            instance_name=instance_name,
            base_dir=DAG_CONFIG["BASE_DIR"],
            command=f"python main.py --source-gs-path {storage_path}",
            dag=dag,
        )

        gce_instance_stop = DeleteGCEOperator(
            task_id=f"{table_config_name}_gce_stop_task", instance_name=instance_name
        )

        (
            export_task
            >> export_bq
            >> gce_instance_start
            >> fetch_install_code
            >> events_export
            >> gce_instance_stop
        )
