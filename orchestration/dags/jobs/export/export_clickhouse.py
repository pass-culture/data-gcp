import datetime
import json

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.bigquery import (
    BigQueryInsertJobOperatorAugmented,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from dependencies.export_clickhouse.export_clickhouse import (
    CLICKHOUSE_ANALYTICS_TRANSFORMATION_CONFIGS,
    CLICKHOUSE_LOADING_CONFIGS,
)

from jobs.crons import SCHEDULE_DICT

GCE_INSTANCE = f"export-clickhouse-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/clickhouse"
GCP_STORAGE_URI = "https://storage.googleapis.com"

DATE = "{{ yyyymmdd(ds) }}"

dag_config = {
    "STORAGE_PATH": f"{DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}",
    "BASE_DIR": "data-gcp/jobs/etl_jobs/internal/export_clickhouse/",
}

gce_params = {
    "instance_name": f"export-clickhouse-data-{ENV_SHORT_NAME}",
    "instance_type": "n1-standard-4",
}

dags = {
    "daily": {
        "schedule": SCHEDULE_DICT["export_clickhouse_daily"],
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2024, 3, 1),
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=20),
            "project_id": GCP_PROJECT_ID,
            "on_failure_callback": on_failure_vm_callback,
            "on_skipped_callback": on_failure_vm_callback,
        },
    },
}


def choose_branch(**context):
    run_id = context["dag_run"].run_id
    if run_id.startswith("scheduled__"):
        return ["waiting_group.waiting_branch"]
    return ["shunt_manual"]


for dag_name, dag_params in dags.items():
    DAG_NAME = f"export_clickhouse_{dag_name}"
    with DAG(
        DAG_NAME,
        default_args=dag_params["default_dag_args"],
        description="Export data to clickhouse",
        schedule=get_airflow_schedule(dag_params["schedule"][ENV_SHORT_NAME]),
        catchup=False,
        start_date=datetime.datetime(2023, 12, 15),
        max_active_runs=1,
        dagrun_timeout=datetime.timedelta(minutes=1440),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        tags=[DAG_TAGS.INCREMENTAL.value, DAG_TAGS.DE.value, DAG_TAGS.VM.value],
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
            "to_days": Param(
                default=0,
                type="integer",
            ),
            "from_days": Param(
                default=-4,
                type="integer",
            ),
        },
    ) as dag:
        branching = BranchPythonOperator(
            task_id="branching",
            python_callable=choose_branch,
            dag=dag,
        )

        with TaskGroup(group_id="waiting_group", dag=dag) as wait_for_daily_tasks:
            wait = EmptyOperator(task_id="waiting_branch", dag=dag)

            wait_for_firebase = delayed_waiting_operator(
                external_dag_id="import_intraday_firebase_data", dag=dag
            )

            wait.set_downstream(wait_for_firebase)

            for table_config in CLICKHOUSE_LOADING_CONFIGS:
                waiting_task = delayed_waiting_operator(
                    dag,
                    external_dag_id="dbt_run_dag",
                    external_task_id=f"data_transformation.{table_config.dbt_model}",
                )
                wait_for_firebase.set_downstream(waiting_task)

        shunt = EmptyOperator(task_id="shunt_manual", dag=dag)
        join = EmptyOperator(task_id="join", dag=dag, trigger_rule="none_failed")

        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            preemptible=False,
            instance_name="{{ params.instance_name }}",
            instance_type="{{ params.instance_type }}",
            retries=2,
            use_gke_network=True,
            labels={"dag_name": DAG_NAME},
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name="{{ params.instance_name }}",
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=dag_config["BASE_DIR"],
            retries=2,
            dag=dag,
        )

        with TaskGroup("loading_stage", dag=dag) as export_tables_tg:
            in_tables_tasks, out_tables_tasks = [], []
            for table_config in CLICKHOUSE_LOADING_CONFIGS:
                _ts = "{{ ts_nodash }}"
                _ds = "{{ ds }}"
                table_id = f"tmp_{_ts}_{table_config.bigquery_table_name}"
                storage_path = f"{dag_config['STORAGE_PATH']}/{table_config.model_name}"

                if table_config.mode == "overwrite":
                    sql_query = f"SELECT * FROM {table_config.bigquery_dataset_name}.{table_config.bigquery_table_name}"
                elif table_config.mode == "incremental":
                    sql_query = (
                        f"SELECT * FROM {table_config.bigquery_dataset_name}.{table_config.bigquery_table_name} "
                        f"WHERE {table_config.partition_key} "
                        + 'BETWEEN DATE("{{ add_days(ds, params.from_days)}}") AND DATE("{{ add_days(ds, params.to_days) }}")'
                    )

                export_task = BigQueryInsertJobOperatorAugmented(
                    project_id=GCP_PROJECT_ID,
                    task_id=f"bigquery_export_{table_config.model_name}",
                    configuration={
                        "query": {
                            "query": sql_query,
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
                in_tables_tasks.append(export_task)

                export_bq = BigQueryInsertJobOperatorAugmented(
                    project_id=GCP_PROJECT_ID,
                    task_id=f"{table_config.model_name}_to_bucket",
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

                clickhouse_export = SSHGCEOperator(
                    dag=dag,
                    task_id=f"{table_config.model_name}_export",
                    instance_name="{{ params.instance_name }}",
                    base_dir=dag_config["BASE_DIR"],
                    command=(
                        "python main.py "
                        f"--source-gs-path {GCP_STORAGE_URI}/{storage_path}/data-*.parquet "
                        f"--table-name {table_config.model_name} "
                        f"--dataset-name {table_config.clickhouse_dataset_name} "
                        f"--update-date {_ds} "
                        f"--mode {table_config.mode}"
                    ),
                )
                export_task >> export_bq >> clickhouse_export
                out_tables_tasks.append(clickhouse_export)

        end_tables = EmptyOperator(task_id="end_tables_export")

        with TaskGroup("analytics_stage", dag=dag) as analytics_tg:
            analytics_task_mapping = {}
            for config in CLICKHOUSE_ANALYTICS_TRANSFORMATION_CONFIGS:
                command = (
                    f"python refresh.py "
                    f"--table-name {config.clickhouse_table_name} "
                    f"--folder {config.clickhouse_dataset_name}"
                )
                if config.ch_session_settings:
                    command += f" --ch-session-settings '{json.dumps(config.ch_session_settings)}'"

                task = SSHGCEOperator(
                    task_id=f"{config.clickhouse_table_name}",
                    instance_name="{{ params.instance_name }}",
                    base_dir=dag_config["BASE_DIR"],
                    command=command,
                    dag=dag,
                )
                analytics_task_mapping[config.clickhouse_table_name] = task

            for config in CLICKHOUSE_ANALYTICS_TRANSFORMATION_CONFIGS:
                for dependency in config.depends_list:
                    if dependency in analytics_task_mapping:
                        (
                            analytics_task_mapping[dependency]
                            >> analytics_task_mapping[config.clickhouse_table_name]
                        )

        gce_instance_stop = DeleteGCEOperator(
            task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
        )

        (
            branching
            >> [shunt, wait_for_daily_tasks]
            >> join
            >> gce_instance_start
            >> fetch_install_code
            >> in_tables_tasks
        )
        (out_tables_tasks >> end_tables >> analytics_tg >> gce_instance_stop)
