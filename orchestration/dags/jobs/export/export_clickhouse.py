from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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
from pathlib import Path
from common.config import (
    GCP_PROJECT_ID,
    DAG_FOLDER,
    ENV_SHORT_NAME,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_TMP_DATASET,
)
from common.utils import get_airflow_schedule
from common.alerts import task_fail_slack_alert
from dependencies.export_clickhouse.export_clickhouse import TABLES_CONFIGS
from common.alerts import task_fail_slack_alert
import copy
from common import macros

SQL_BASE_PATH = Path("dependencies/export_clickhouse/sql")

GCE_INSTANCE = f"export-clickhouse-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/clickhouse"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

default_dag_args = {
    "start_date": datetime.datetime(2023, 9, 1),
    "retries": 1,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": datetime.timedelta(minutes=5),
    "project_id": GCP_PROJECT_ID,
}
DATE = "{{ yyyymmdd(ds) }}"

dag_config = {
    "STORAGE_PATH": f"{DATA_GCS_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}",
    "BASE_DIR": "data-gcp/jobs/etl_jobs/internal/export_clickhouse/",
}

gce_params = {
    "instance_name": f"export-clickhouse-data-{ENV_SHORT_NAME}",
    "instance_type": "n1-standard-4",
}

dags = {
    "daily": {
        "schedule_interval": {"prod": "0 8 * * *", "dev": None, "stg": None},
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2024, 3, 1),
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=20),
            "project_id": GCP_PROJECT_ID,
        },
    },
}


for dag_name, dag_params in dags.items():
    with DAG(
        f"export_clickhouse_{dag_name}",
        default_args=dag_params["default_dag_args"],
        description="Export to analytics data posthog",
        schedule_interval=get_airflow_schedule(
            dag_params["schedule_interval"][ENV_SHORT_NAME]
        ),
        catchup=False,
        start_date=datetime.datetime(2023, 12, 15),
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
                default=gce_params["instance_type"],
                type="string",
            ),
            "instance_name": Param(
                default=gce_params["instance_name"],
                type="string",
            ),
            "days": Param(
                default=4,
                type="integer",
            ),
        },
    ) as dag:

        gce_instance_start = StartGCEOperator(
            task_id=f"gce_start_task",
            preemptible=False,
            instance_name="{{ params.instance_name }}",
            instance_type="{{ params.instance_type }}",
            retries=2,
        )

        fetch_code = CloneRepositoryGCEOperator(
            task_id=f"fetch_code",
            instance_name="{{ params.instance_name }}",
            python_version="3.10",
            command="{{ params.branch }}",
            retries=2,
        )

        install_dependencies = SSHGCEOperator(
            task_id=f"install_dependencies",
            instance_name="{{ params.instance_name }}",
            base_dir=dag_config["BASE_DIR"],
            command="pip install -r requirements.txt --user",
            dag=dag,
        )

        in_tables_tasks, out_tables_tasks = [], []
        for table_config in TABLES_CONFIGS:
            tmp_table_name = table_config["sql"]
            clickhouse_table_name = table_config["clickhouse_table_name"]
            clickhouse_dataset_name = table_config["clickhouse_dataset_name"]
            mode = table_config["mode"]
            _ts = "{{ ts_nodash }}"
            _ds = "{{ ds }}"
            table_id = f"tmp_{_ts}_{tmp_table_name}"
            storage_path = f"{dag_config['STORAGE_PATH']}/{clickhouse_table_name}"

            export_task = BigQueryExecuteQueryOperator(
                task_id=f"bigquery_export_{clickhouse_table_name}",
                sql=(SQL_BASE_PATH / f"{tmp_table_name}.sql").as_posix(),
                write_disposition="WRITE_TRUNCATE",
                use_legacy_sql=False,
                destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{table_id}",
                dag=dag,
            )

            export_bq = BigQueryInsertJobOperator(
                task_id=f"{clickhouse_table_name}_to_bucket",
                configuration={
                    "extract": {
                        "sourceTable": {
                            "projectId": GCP_PROJECT_ID,
                            "datasetId": BIGQUERY_TMP_DATASET,
                            "tableId": table_id,
                        },
                        "compression": None,
                        "destinationUris": f"gs://{storage_path}/data-*.parquet",
                        "destinationFormat": "PARQUET",
                    }
                },
                dag=dag,
            )

            events_export = SSHGCEOperator(
                task_id=f"{clickhouse_table_name}_events_export",
                instance_name="{{ params.instance_name }}",
                base_dir=dag_config["BASE_DIR"],
                command="python main.py "
                f"--source-gs-path https://storage.googleapis.com/{storage_path}/data-*.parquet --table-name {clickhouse_table_name} --dataset-name {clickhouse_dataset_name} --update-date {_ds} --mode {mode}",
                dag=dag,
            )
            export_task >> export_bq >> events_export
            in_tables_tasks.append(export_task)
            out_tables_tasks.append(events_export)

        end_tables = DummyOperator(task_id="end_tables_export")

        gce_instance_stop = StopGCEOperator(
            task_id=f"gce_stop_task", instance_name="{{ params.instance_name }}"
        )

        (gce_instance_start >> fetch_code >> install_dependencies >> in_tables_tasks)
        (out_tables_tasks >> end_tables >> gce_instance_stop)
