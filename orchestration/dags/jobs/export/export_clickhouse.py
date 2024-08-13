import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.alerts import task_fail_slack_alert
from common.config import (
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DATA_GCS_BUCKET_NAME,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
)
from common.operators.gce import (
    CloneRepositoryGCEOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import get_airflow_schedule, waiting_operator
from dependencies.export_clickhouse.export_clickhouse import (
    TABLES_CONFIGS,
    VIEWS_CONFIGS,
)

GCE_INSTANCE = f"export-clickhouse-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/clickhouse"
GCP_STORAGE_URI = "https://storage.googleapis.com"
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
        "schedule_interval": {
            "prod": "0 1 * * *",
            "stg": "0 1 * * *",
            "dev": "0 1 * * *",
        },
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2024, 3, 1),
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=20),
            "project_id": GCP_PROJECT_ID,
        },
    },
}


def choose_branch(**context):
    run_id = context["dag_run"].run_id
    if run_id.startswith("scheduled__"):
        return ["waiting_dbt_group.waiting_dbt_jobs"]
    return ["shunt_manual"]


for dag_name, dag_params in dags.items():
    with DAG(
        f"export_clickhouse_{dag_name}",
        default_args=dag_params["default_dag_args"],
        description="Export data to clickhouse",
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
            provide_context=True,
            dag=dag,
        )

        with TaskGroup(
            group_id="waiting_dbt_group", dag=dag
        ) as wait_for_dbt_daily_tasks:
            wait = DummyOperator(task_id="waiting_dbt_jobs", dag=dag)

            for table_config in TABLES_CONFIGS:
                waiting_task = waiting_operator(
                    dag,
                    "dbt_run_dag",
                    f"data_transformation.{table_config['dbt_model']}",
                )
                wait.set_downstream(waiting_task)

        shunt = DummyOperator(task_id="shunt_manual", dag=dag)
        join = DummyOperator(task_id="join", dag=dag, trigger_rule="none_failed")

        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            preemptible=False,
            instance_name="{{ params.instance_name }}",
            instance_type="{{ params.instance_type }}",
            retries=2,
            gce_network_type="GKE",
        )

        fetch_code = CloneRepositoryGCEOperator(
            task_id="fetch_code",
            instance_name="{{ params.instance_name }}",
            python_version="3.10",
            command="{{ params.branch }}",
            retries=2,
        )

        install_dependencies = SSHGCEOperator(
            task_id="install_dependencies",
            instance_name="{{ params.instance_name }}",
            base_dir=dag_config["BASE_DIR"],
            command="pip install -r requirements.txt --user",
            dag=dag,
        )

        in_tables_tasks, out_tables_tasks = [], []
        for table_config in TABLES_CONFIGS:
            table_name = table_config["bigquery_table_name"]
            dataset_name = table_config["bigquery_dataset_name"]
            partition_key = table_config.get("partition_key", "partition_date")
            clickhouse_table_name = table_config["clickhouse_table_name"]
            clickhouse_dataset_name = table_config["clickhouse_dataset_name"]

            mode = table_config["mode"]
            _ts = "{{ ts_nodash }}"
            _ds = "{{ ds }}"
            table_id = f"tmp_{_ts}_{table_name}"
            storage_path = f"{dag_config['STORAGE_PATH']}/{clickhouse_table_name}"

            if mode == "overwrite":
                sql_query = f"""SELECT * FROM {dataset_name}.{table_name} """
            elif mode == "incremental":
                sql_query = (
                    f"""SELECT * FROM {dataset_name}.{table_name}  WHERE {partition_key} """
                    + ' BETWEEN DATE("{{ add_days(ds, params.from_days)}}") AND DATE("{{ add_days(ds, params.to_days) }}")'
                )

            export_task = BigQueryExecuteQueryOperator(
                task_id=f"bigquery_export_{clickhouse_table_name}",
                sql=sql_query,
                write_disposition="WRITE_TRUNCATE",
                use_legacy_sql=False,
                destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{table_id}",
                dag=dag,
            )
            in_tables_tasks.append(export_task)

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

            clickhouse_export = SSHGCEOperator(
                dag=dag,
                task_id=f"{clickhouse_table_name}_export",
                instance_name="{{ params.instance_name }}",
                base_dir=dag_config["BASE_DIR"],
                command="python main.py "
                f"--source-gs-path {GCP_STORAGE_URI}/{storage_path}/data-*.parquet "
                f"--table-name {clickhouse_table_name} "
                f"--dataset-name {clickhouse_dataset_name} "
                f"--update-date {_ds} "
                f"--mode {mode} ",
            )
            export_task >> export_bq >> clickhouse_export
            out_tables_tasks.append(clickhouse_export)

        end_tables = DummyOperator(task_id="end_tables_export")
        views_refresh = []
        for view_config in VIEWS_CONFIGS:
            clickhouse_table_name = view_config["clickhouse_table_name"]
            view_task = SSHGCEOperator(
                task_id=f"refresh_{clickhouse_table_name}",
                instance_name="{{ params.instance_name }}",
                base_dir=dag_config["BASE_DIR"],
                command="python refresh.py " f"--table-name {clickhouse_table_name}",
                dag=dag,
            )
            views_refresh.append(view_task)

        gce_instance_stop = StopGCEOperator(
            task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
        )

        (
            branching
            >> [shunt, wait_for_dbt_daily_tasks]
            >> join
            >> gce_instance_start
            >> fetch_code
            >> install_dependencies
            >> in_tables_tasks
        )
        (out_tables_tasks >> end_tables >> views_refresh >> gce_instance_stop)
