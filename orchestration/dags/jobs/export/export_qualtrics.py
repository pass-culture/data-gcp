import datetime

from airflow import DAG
from airflow.models import Param
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import BigQueryInsertJobOperatorAugmented
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

DAG_NAME = "export_qualtrics"
GCE_INSTANCE = f"export-qualtrics-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/qualtrics"
EXPORT_DATASET = f"export_{ENV_SHORT_NAME}"

dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2022, 6, 24),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_vm_callback,
}

with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Export user data to Qualtrics mailing lists",
    schedule=get_airflow_schedule("00 06 25 * *"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=120),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "beneficiary_dataset": Param(
            default=BIGQUERY_TMP_DATASET,
            type="string",
        ),
        "beneficiary_table": Param(
            default="export_qualtrics_beneficiary",
            type="string",
        ),
        "venue_dataset": Param(
            default=BIGQUERY_TMP_DATASET,
            type="string",
        ),
        "venue_table": Param(
            default="export_qualtrics_venue",
            type="string",
        ),
    },
    tags=[DAG_TAGS.DE.value, DAG_TAGS.VM.value],
) as dag:
    gce_instance_start = StartGCEOperator(
        instance_name=GCE_INSTANCE,
        task_id="gce_start_task",
        labels={"job_type": "long_task", "dag_name": DAG_NAME},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name=GCE_INSTANCE,
        branch="{{ params.branch }}",
        base_dir=BASE_PATH,
        python_version="3.13",
        retries=2,
    )

    prepare_beneficiary_tmp = BigQueryInsertJobOperatorAugmented(
        task_id="prepare_beneficiary_tmp",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    WITH user_email AS (
                        SELECT * FROM EXTERNAL_QUERY(
                            '{APPLICATIVE_EXTERNAL_CONNECTION_ID}',
                            'SELECT CAST("id" AS varchar(255)) AS user_id, email FROM public.user'
                        )
                    )
                    SELECT t.*, ue.email
                    FROM `{GCP_PROJECT_ID}.{EXPORT_DATASET}.qualtrics_beneficiary_account` t
                    LEFT JOIN user_email ue ON ue.user_id = t.user_id
""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": "{{ params.beneficiary_dataset }}",
                    "tableId": "{{ params.beneficiary_table }}_{{ ds_nodash }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    prepare_venue_tmp = BigQueryInsertJobOperatorAugmented(
        task_id="prepare_venue_tmp",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    SELECT *
                    FROM `{GCP_PROJECT_ID}.{EXPORT_DATASET}.qualtrics_venue_account`
                """,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": "{{ params.venue_dataset }}",
                    "tableId": "{{ params.venue_table }}_{{ ds_nodash }}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    export_beneficiary = SSHGCEOperator(
        task_id="export_beneficiary_to_qualtrics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py --task export_beneficiary --ds {{ ds }} --dataset-name {{ params.beneficiary_dataset }} --table-name {{ params.beneficiary_table }}_{{ ds_nodash }}",
        deferrable=True,
        do_xcom_push=True,
    )

    export_venue = SSHGCEOperator(
        task_id="export_venue_to_qualtrics",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_PATH,
        environment=dag_config,
        command="uv run python main.py --task export_venue --ds {{ ds }} --dataset-name {{ params.venue_dataset }} --table-name {{ params.venue_table }}_{{ ds_nodash }}",
        deferrable=True,
        do_xcom_push=True,
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE
    )

    (
        gce_instance_start
        >> fetch_install_code
        >> [prepare_beneficiary_tmp, prepare_venue_tmp]
    )
    prepare_beneficiary_tmp >> export_beneficiary >> gce_instance_stop
    prepare_venue_tmp >> export_venue >> gce_instance_stop
