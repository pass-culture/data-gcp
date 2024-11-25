import datetime
import logging

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
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
    StopGCEOperator,
)
from common.utils import delayed_waiting_operator, get_airflow_schedule
from dependencies.export_clickhouse.export_clickhouse import (
    ANALYTICS_CONFIGS,
    TABLES_CONFIGS,
)
from jobs.crons import schedule_dict

from airflow import DAG
from airflow.decorators import task  # For Airflow 2.3+ task decorator
from airflow.models import Param
from airflow.operators.empty import EmptyOperator  # Airflow 2.3+ uses EmptyOperator
from airflow.operators.python import BranchPythonOperator  # Add this import
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryInsertJobOperator,
)
from airflow.utils.task_group import TaskGroup

GCE_INSTANCE = f"export-clickhouse-{ENV_SHORT_NAME}"
BASE_PATH = "data-gcp/jobs/etl_jobs/internal/clickhouse"
GCP_STORAGE_URI = "https://storage.googleapis.com"
dag_config = {
    "PROJECT_NAME": GCP_PROJECT_ID,
    "ENV_SHORT_NAME": ENV_SHORT_NAME,
}

DATE = "{{ yyyymmdd(ds) }}"

dag_config.update(
    {
        "STORAGE_PATH": f"{DATA_GCS_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}",
        "BASE_DIR": "data-gcp/jobs/etl_jobs/internal/export_clickhouse/",
    }
)

gce_params = {
    "instance_name": f"export-clickhouse-data-{ENV_SHORT_NAME}",
    "instance_type": "n1-standard-4",
}


dags = {
    "batch": {
        "schedule_interval": schedule_dict["clickhouse_exports"]["batch"],
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2024, 3, 1),
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=20),
            # "project_id": GCP_PROJECT_ID,
            "on_failure_callback": task_fail_slack_alert,
            "on_skipped_callback": task_fail_slack_alert,
        },
    },
}


def choose_branch(**context):
    run_id = context["dag_run"].run_id
    if run_id.startswith("scheduled__"):
        return ["waiting_group.waiting_branch"]
    return ["shunt_manual"]


def generate_date_batches(from_date, to_date, batch_size):
    """Generate date ranges split into batches."""
    date_batches = []
    delta_days = (to_date - from_date).days + 1  # Inclusive of both dates
    if delta_days <= 0:
        return date_batches  # Return empty list if date range is invalid
    num_batches = (delta_days + batch_size - 1) // batch_size  # Ceiling division

    for batch_num in range(num_batches):
        batch_start_date = from_date + datetime.timedelta(days=batch_num * batch_size)
        batch_end_date = min(
            batch_start_date + datetime.timedelta(days=batch_size - 1), to_date
        )
        date_batches.append(
            {
                "batch_number": batch_num + 1,
                "start_date": batch_start_date.strftime("%Y-%m-%d"),
                "end_date": batch_end_date.strftime("%Y-%m-%d"),
            }
        )
    return date_batches


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
        tags=["Incremental", "Sandbox"],
        render_template_as_native_obj=True,
        params={
            "branch": Param(
                default="production" if ENV_SHORT_NAME == "prod" else "master",
                type_="string",
            ),
            "instance_type": Param(
                default=gce_params["instance_type"],
                type_="string",
            ),
            "instance_name": Param(
                default=gce_params["instance_name"],
                type_="string",
            ),
            "to_date": Param(
                default="{{ ds }}",
                type="string",
                format="date",
            ),
            "from_date": Param(
                default="{{ (ds - macros.timedelta(days=365)).strftime('%Y-%m-%d') }}",
                type="string",
                format="date",
            ),
            "batch_size": Param(
                default=90,
                type_="integer",
                description="Number of days in a batch (ClickHouse max=100)",
            ),
        },
    ) as dag:
        branching = BranchPythonOperator(
            task_id="branching",
            python_callable=choose_branch,
            provide_context=True,
            dag=dag,
        )

        with TaskGroup(group_id="waiting_group", dag=dag) as wait_for_daily_tasks:
            wait = EmptyOperator(task_id="waiting_branch", dag=dag)

            wait_for_firebase = delayed_waiting_operator(
                external_dag_id="import_intraday_firebase_data", dag=dag
            )

            wait.set_downstream(wait_for_firebase)

            for table_config in TABLES_CONFIGS:
                waiting_task = delayed_waiting_operator(
                    dag,
                    external_dag_id="dbt_run_dag",
                    external_task_id=f"data_transformation.{table_config['dbt_model']}",
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
        )

        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name="{{ params.instance_name }}",
            branch="{{ params.branch }}",
            installer="uv",
            python_version="'3.10'",
            base_dir=dag_config["BASE_DIR"],
            retries=2,
            dag=dag,
        )

        @task
        def get_date_batches(**kwargs):
            context = kwargs
            ti = context["ti"]
            from_date_template = context["params"]["from_date"]
            to_date_template = context["params"]["to_date"]
            batch_size = context["params"]["batch_size"]

            # Render the templates using ti.task.render_template
            from_date_str = ti.task.render_template(from_date_template, context)
            to_date_str = ti.task.render_template(to_date_template, context)

            # Now parse the dates
            from_date = datetime.datetime.strptime(from_date_str, "%Y-%m-%d")
            to_date = datetime.datetime.strptime(to_date_str, "%Y-%m-%d")

            logging.info(f"Computed from_date: {from_date}")
            logging.info(f"Computed to_date: {to_date}")

            # Now proceed as before
            date_batches = generate_date_batches(from_date, to_date, batch_size)
            return date_batches

        date_batches = get_date_batches()

        @task
        def generate_batches(table_configs, date_batches):
            batches = []
            for table_config in table_configs:
                for batch in date_batches:
                    batch_dict = {
                        "table_config": {
                            # Only include relevant keys
                            key: table_config[key]
                            for key in [
                                "dbt_model",
                                "bigquery_dataset_name",
                                "bigquery_table_name",
                                "clickhouse_table_name",
                                "clickhouse_dataset_name",
                                "mode",
                                "partition_key",
                            ]
                        },
                        "batch": batch,
                    }
                    logging.info(f"Generated batch: {batch_dict}")
                    batches.append(batch_dict)
            return batches

        batches = generate_batches(TABLES_CONFIGS, date_batches)

        @task(task_id="process_batches")
        def process_batches(table_config, batch, **kwargs):
            logging.info(f"Received kwargs: {kwargs}")
            logging.info(f"Processing table_config: {table_config}")
            logging.info(f"Processing batch: {batch}")
            table_name = table_config["bigquery_table_name"]
            dataset_name = table_config["bigquery_dataset_name"]
            partition_key = table_config["partition_key"]
            clickhouse_table_name = table_config["clickhouse_table_name"]
            clickhouse_dataset_name = table_config["clickhouse_dataset_name"]
            mode = table_config["mode"]

            _ts = "{{ ts_nodash }}"
            _ds = "{{ ds }}"
            batch_number = batch["batch_number"]
            batch_start_date = batch["start_date"]
            batch_end_date = batch["end_date"]
            table_id = f"tmp_{_ts}_{table_name}_{batch_number}"
            storage_path = f"{dag_config['STORAGE_PATH']}/{clickhouse_table_name}"

            if mode == "overwrite":
                sql_query = f"""SELECT * FROM {dataset_name}.{table_name} """
            elif mode == "incremental":
                sql_query = f"""
                SELECT * FROM {dataset_name}.{table_name}
                WHERE {partition_key} BETWEEN DATE('{batch_start_date}')
                AND DATE('{batch_end_date}')
                """

            export_task = BigQueryExecuteQueryOperator(
                task_id=f"bigquery_export_{clickhouse_table_name}_batch_{batch_number}",
                sql=sql_query,
                write_disposition="WRITE_TRUNCATE",
                use_legacy_sql=False,
                destination_dataset_table=f"{BIGQUERY_TMP_DATASET}.{table_id}",
                dag=dag,
            )

            export_bq = BigQueryInsertJobOperator(
                task_id=f"{clickhouse_table_name}_to_bucket_batch_{batch_number}",
                configuration={
                    "extract": {
                        "sourceTable": {
                            "projectId": GCP_PROJECT_ID,
                            "datasetId": BIGQUERY_TMP_DATASET,
                            "tableId": table_id,
                        },
                        "compression": None,
                        "destinationUris": [f"gs://{storage_path}/data-*.parquet"],
                        "destinationFormat": "PARQUET",
                    }
                },
                dag=dag,
            )

            clickhouse_export = SSHGCEOperator(
                dag=dag,
                task_id=f"{clickhouse_table_name}_export_batch_{batch_number}",
                instance_name="{{ params.instance_name }}",
                base_dir=dag_config["BASE_DIR"],
                installer="uv",
                command="python main.py "
                f"--source-gs-path {GCP_STORAGE_URI}/{storage_path}/data-*.parquet "
                f"--table-name {clickhouse_table_name} "
                f"--dataset-name {clickhouse_dataset_name} "
                f"--update-date {_ds} "
                f"--mode {mode} ",
            )

            export_task >> export_bq >> clickhouse_export
            return "Done"

        logging.info(f"Sanitized batches being passed to process_batches: {batches}")
        process_batches_tasks = process_batches.expand_kwargs(batches)

        # After all tasks are done
        wait_for_batches = EmptyOperator(task_id="wait_for_batches", dag=dag)
        process_batches_tasks >> wait_for_batches

        with TaskGroup("analytics_stage", dag=dag) as analytics_tg:
            analytics_task_mapping = {}
            for config in ANALYTICS_CONFIGS:
                clickhouse_table_name = config["clickhouse_table_name"]
                clickhouse_folder_name = config["clickhouse_dataset_name"]

                task = SSHGCEOperator(
                    task_id=f"{clickhouse_table_name}",
                    instance_name="{{ params.instance_name }}",
                    base_dir=dag_config["BASE_DIR"],
                    installer="uv",
                    command=f"python refresh.py --table-name {clickhouse_table_name} --folder {clickhouse_folder_name}",
                    dag=dag,
                )
                analytics_task_mapping[clickhouse_table_name] = task

            for config in ANALYTICS_CONFIGS:
                clickhouse_table_name = config["clickhouse_table_name"]
                # Set upstream dependencies if the config has a "depends_list"
                if "depends_list" in config:
                    for dependency in config["depends_list"]:
                        # Set the upstream dependency
                        if dependency in analytics_task_mapping:
                            (
                                analytics_task_mapping[dependency]
                                >> analytics_task_mapping[clickhouse_table_name]
                            )

        wait_for_batches >> analytics_tg

        gce_instance_stop = StopGCEOperator(
            task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
        )

        analytics_tg >> gce_instance_stop

        # Define the DAG dependencies
        (
            branching
            >> [shunt, wait_for_daily_tasks]
            >> join
            >> gce_instance_start
            >> fetch_install_code
            >> date_batches
            >> batches
            >> process_batches_tasks
        )
