import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.access_gcp_secrets import access_secret_data
from common.callback import on_failure_base_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from dependencies.export_vidoc.export_vidoc import S3_SECRET_NAME, TABLES_CONFIGS
from dependencies.export_vidoc.s3_utils import (
    init_s3_client,
    upload_gcs_prefix_to_s3,
    wipe_s3_prefix,
)

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "export_vidoc_daily"
GCS_EXPORT_ROOT = f"vidoc_export/{ENV_SHORT_NAME}"


def _transfer_to_s3(gcs_prefix: str, s3_prefix: str, secret_name: str, **_):
    s3_config = access_secret_data(GCP_PROJECT_ID, secret_name, as_dict=True)
    if not s3_config:
        raise RuntimeError(f"Secret {secret_name} is missing or empty")
    s3_client = init_s3_client(s3_config)
    s3_bucket = s3_config["target_s3_name"]
    wipe_s3_prefix(s3_client, s3_bucket, f"{s3_prefix}/")
    upload_gcs_prefix_to_s3(
        gcs_bucket=DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
        gcs_prefix=gcs_prefix,
        s3_client=s3_client,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
    )


def _choose_branch(**context):
    run_id = context["dag_run"].run_id
    if run_id.startswith("scheduled__"):
        return ["waiting_group.waiting_branch"]
    return ["shunt_manual"]


default_dag_args = {
    "start_date": datetime.datetime(2026, 4, 21),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
    "project_id": GCP_PROJECT_ID,
    "on_failure_callback": on_failure_base_callback,
}


with DAG(
    DAG_NAME,
    default_args=default_dag_args,
    description="Export exp_vidoc__* BigQuery tables to the SNUM OVH-S3 bucket",
    schedule_interval=get_airflow_schedule(
        SCHEDULE_DICT["export_vidoc_daily"][ENV_SHORT_NAME]
    ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=datetime.timedelta(minutes=60),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DE.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "s3_secret_name": Param(default=S3_SECRET_NAME, type="string"),
    },
) as dag:
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_choose_branch,
        provide_context=True,
    )

    with TaskGroup(group_id="waiting_group") as waiting_group:
        wait = EmptyOperator(task_id="waiting_branch")
        for config in TABLES_CONFIGS:
            wait >> delayed_waiting_operator(
                dag,
                external_dag_id="dbt_run_dag",
                external_task_id=f"data_transformation.{config['dbt_model']}",
            )

    shunt = EmptyOperator(task_id="shunt_manual")
    join = EmptyOperator(task_id="join", trigger_rule="none_failed")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    branching >> [shunt, waiting_group] >> join

    for config in TABLES_CONFIGS:
        s3_prefix = config["s3_prefix"]
        gcs_prefix = f"{GCS_EXPORT_ROOT}/{{{{ ds_nodash }}}}/{s3_prefix}"

        bq_to_gcs = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id=f"bq_extract_{s3_prefix}",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": config["bigquery_dataset_name"],
                        "tableId": config["bigquery_table_name"],
                    },
                    "compression": None,
                    "destinationUris": (
                        f"gs://{DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME}/{gcs_prefix}/data-*.parquet"
                    ),
                    "destinationFormat": "PARQUET",
                }
            },
        )

        gcs_to_s3 = PythonOperator(
            task_id=f"gcs_to_s3_{s3_prefix}",
            python_callable=_transfer_to_s3,
            op_kwargs={
                "gcs_prefix": gcs_prefix,
                "s3_prefix": s3_prefix,
                "secret_name": "{{ params.s3_secret_name }}",
            },
        )

        join >> bq_to_gcs >> gcs_to_s3 >> end
