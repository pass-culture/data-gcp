import datetime

from common.access_gcp_secrets import access_secret_data
from common.config import (
    DAG_TAGS,
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
from common.utils import (
    build_export_context,
    delayed_waiting_operator,
    get_json_from_gcs,
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

default_args = {
    "start_date": datetime.datetime(2020, 12, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=2),
    "project_id": GCP_PROJECT_ID,
}

GCE_INSTANCE = f"encrypted-export-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/external/encrypted_exports"
BASE_BUCKET = f"data-partners-export-bucket-{ENV_SHORT_NAME}"

partner_dict = get_json_from_gcs(BASE_BUCKET, "partners_names.json")
DAG_NAME = "dbt_encrypted_export"

for partner_id, partner_name in partner_dict.items():
    with DAG(
        f"{DAG_NAME}_{partner_name}",
        default_args=default_args,
        dagrun_timeout=datetime.timedelta(minutes=180),
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
                default=" --no-write-json ",
                type="string",
            ),
            "instance_type": Param(
                default="n1-standard-32"
                if ENV_SHORT_NAME == "prod"
                else "n1-standard-2",
                type="string",
            ),
        },
        tags=[DAG_TAGS.DBT.value, DAG_TAGS.DE.value],
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

        quality_tests = dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"bash {PATH_TO_DBT_PROJECT}/scripts/dbt_test.sh ",
            env={
                "SELECT": "tag:export",
                "GLOBAL_CLI_FLAGS": "{{ params.GLOBAL_CLI_FLAGS }}",
                "target": "{{ params.target }}",
                "PATH_TO_DBT_TARGET": PATH_TO_DBT_TARGET,
                "ENV_SHORT_NAME": ENV_SHORT_NAME,
                "EXCLUSION": "audit elementary",
            },
            append_env=True,
            cwd=PATH_TO_DBT_PROJECT,
            dag=dag,
            retries=0,
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
                    "export_schema: tmp_export_{{ ti.xcom_pull(task_ids='build_export_context', key='partner_name') }}, "
                    "export_schema_expiration_day : 1,"
                    f"secret_partner_value: '{access_secret_data(GCP_PROJECT_ID, f'dbt_export_private_partner_salt_{partner_name}')}', "
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
            instance_type="{{ params.instance_type }}",
            preemptible=False,
            disk_size_gb=100,
            labels={"job_type": "long_task", "dag_name": DAG_NAME},
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
                f"--encryption-key '{access_secret_data(GCP_PROJECT_ID, f'dbt_export_encryption_key_{partner_name}')}' "
            ),
        )

        parquet_transfer = SSHGCEOperator(
            task_id="parquet_transfer",
            instance_name=f"{GCE_INSTANCE}-{partner_name}",
            base_dir=BASE_PATH,
            command=(
                "python main.py transfer "
                "--partner-name \"{{ ti.xcom_pull(task_ids='build_export_context', key='partner_name') }}\" "
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
            >> quality_tests
            >> bq_obfuscation
            >> export_group
            >> gce_instance_start
            >> fetch_install_code
            >> parquet_encryption
            >> parquet_transfer
            >> gce_instance_stop
        )
