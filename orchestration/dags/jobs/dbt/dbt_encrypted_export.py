import datetime

from common.access_gcp_secrets import access_secret_data
from common.config import (
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    PATH_TO_DBT_PROJECT,
    PATH_TO_DBT_TARGET,
)
from common.operators.gce import (
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from jobs.crons import ENCRYPTED_EXPORT_DICT

from airflow import DAG
from airflow.models import Param
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.utils.task_group import TaskGroup
from dags.common.utils import (
    build_export_context,
    delayed_waiting_operator,
    get_json_from_gcs,
)

default_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

GCE_INSTANCE = f"encrypted-export-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/encrypted_exports"
BASE_BUCKET = f"data-partners-export-bucket-{ENV_SHORT_NAME}"  # "data-bucket-dev"

partner_dict = get_json_from_gcs(BASE_BUCKET, "partners_names.json")
dag_name = "dbt_encrypted_export"

for partner_id, partner_name in partner_dict.items():
    with DAG(
        f"{dag_name}_{partner_name}",
        default_args=default_args,
        dagrun_timeout=datetime.timedelta(minutes=60),
        catchup=False,
        description=f"Generate obfuscated export for {partner_name}",
        schedule_interval=ENCRYPTED_EXPORT_DICT.get(partner_id, {}).get(
            ENV_SHORT_NAME, None
        ),
        params={
            "branch": Param(
                default="production" if ENV_SHORT_NAME == "prod" else "master",
                type="string",
            ),
            "target": Param(
                default=ENV_SHORT_NAME,
                type="string",
            ),
            "GLOBAL_CLI_FLAGS": Param(
                default="--no-write-json",
                type="string",
            ),
        },
    ) as dag:
        build_context = PythonOperator(
            task_id="build_export_context",
            python_callable=build_export_context,
            op_kwargs={
                "partner_name": partner_name,
                "bucket": BASE_BUCKET,
                "export_date": "{{ ds_nodash }}",
                "parquet_storage_gcs_bucket": BASE_BUCKET,
            },
        )

        wait_for_dbt_daily = delayed_waiting_operator(
            dag=dag, external_dag_id="dbt_run_dag", skip_manually_triggered=True
        )

        bq_obfuscation = BashOperator(
            task_id="bq_obfuscation",
            bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_run_operation.sh ",
            env={
                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                "target": "{{ params.target }}",
                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                "operation": "generate_export_tables",
                "args": (
                    "{"
                    "export_tables: {{ ti.xcom_pull(task_ids='build_export_context', key='table_list') | tojson }}, "
                    "export_schema: export_{{ ti.xcom_pull(task_ids='build_export_context', key='partner_name') }}, "
                    f"secret_partner_value: '{access_secret_data(GCP_PROJECT_ID,f'dbt_export_private_partner_salt_{partner_name}')}', "
                    "fields_obfuscation_config: {{ ti.xcom_pull(task_ids='build_export_context', key='obfuscation_config').obfuscated_fields | tojson if ti.xcom_pull(task_ids='build_export_context', key='obfuscation_config') else '{}' }}"
                    "}"
                ),
            },
            cwd=PATH_TO_DBT_PROJECT,
            append_env=True,
        )

        with TaskGroup(group_id="export_group") as export_group:
            dynamic_tasks = BigQueryToGCSOperator.partial(
                task_id="export_bq_to_gcs",
                export_format="PARQUET",
            ).expand_kwargs(XComArg(build_context))

        gce_instance_start = StartGCEOperator(
            instance_name=f"{GCE_INSTANCE}-{partner_name}",
            task_id="gce_start_task",
            instance_type="n1-highmem-2",
            preemptible=True,
            disk_size_gb=100,
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=f"{GCE_INSTANCE}-{partner_name}",
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=BASE_PATH,
        )

        parquet_encryption = SSHGCEOperator(
            task_id="parquet_encryption",
            instance_name=f"{GCE_INSTANCE}-{partner_name}",
            base_dir=BASE_PATH,
            command=(
                "python main.py encrypt "
                "--partner-name \"{{ ti.xcom_pull(task_ids='build_export_context', key='partner_name') }}\" "
                f'--gcs-bucket "{BASE_BUCKET}" '
                "--export-date \"{{ ti.xcom_pull(task_ids='build_export_context', key='export_date') }}\" "
                "--table-list '{{ ti.xcom_pull(task_ids='build_export_context', key='table_list') | tojson }}' "
                f"--encryption-key '{access_secret_data(GCP_PROJECT_ID,f'dbt_export_encryption_key_{partner_name}')}' "
            ),
        )

        parquet_transfer = SSHGCEOperator(
            task_id="parquet_transfer",
            instance_name=f"{GCE_INSTANCE}-{partner_name}",
            base_dir=BASE_PATH,
            command=(
                "python main.py transfer "
                "--partner-name \"{{ ti.xcom_pull(task_ids='build_export_context', key='partner_name') }}\" "
                "--target-bucket-config \"{{ ti.xcom_pull(task_ids='build_export_context', key='target_bucket_config') }}\" "
                f'--gcs-bucket "{BASE_BUCKET}" '
                "--export-date \"{{ ti.xcom_pull(task_ids='build_export_context', key='export_date') }}\" "
                "--table-list '{{ ti.xcom_pull(task_ids='build_export_context', key='table_list') | tojson }}' "
            ),
        )

        gce_instance_stop = StopGCEOperator(
            task_id="gce_stop_task", instance_name=f"{GCE_INSTANCE}-{partner_name}"
        )

        (
            wait_for_dbt_daily
            >> build_context
            >> bq_obfuscation
            >> export_group
            >> gce_instance_start
            >> fetch_install_code
            >> parquet_encryption
            >> parquet_transfer
            >> gce_instance_stop
        )
