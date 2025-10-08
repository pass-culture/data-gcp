import json
import logging
import os
import time
from datetime import timedelta, datetime


import requests
from common.access_gcp_secrets import create_key_if_not_exists
from common.config import (
    GCP_PROJECT_ID,
    LOCAL_ENV,
)
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2 import id_token

from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.types import DagRunType
from common.dbt.dag_utils import load_manifest


@provide_session
def get_last_execution_date(
    dag_id, upper_date_limit, lower_date_limit=None, session=None
):
    """
    Query the last execution_date (logical date) of the specified external DAG between lower and upper date limits.

    :param dag_id: The DAG ID of the DAG to query.
    :upper_date_limit: The upper bound on the execution_date (logical date) to search for.
    :lower_date_limit: The lower bound on the execution_date (logical date) to search for.
    :param session: The database session.
    :return: The last execution_date (logical date) or None if no runs are found.
    """
    logging.info(f"Querying last execution date for DAG: {dag_id}")
    logging.info(f"Upper date limit: {upper_date_limit.replace(tzinfo=None)}")
    logging.info(f"Lower date limit: {lower_date_limit.replace(tzinfo=None)}")

    query = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.run_type == DagRunType.SCHEDULED,
    )

    if lower_date_limit:
        query = query.filter(DagRun.execution_date >= lower_date_limit)

    query = query.filter(DagRun.execution_date <= upper_date_limit)

    last_dag_run = query.order_by(DagRun.execution_date.desc()).first()

    if last_dag_run:
        logging.info(f"Last scheduled {dag_id} found:")
        logging.info(
            f"Execution date:   {last_dag_run.execution_date.replace(tzinfo=None)}"
        )
        return last_dag_run.execution_date
    else:
        logging.warning(
            f"No {dag_id} scheduled DAG runs found between {lower_date_limit.replace(tzinfo=None)} and {upper_date_limit.replace(tzinfo=None)}"
        )
        return None


def getting_service_account_token(function_name):
    function_url = (
        f"https://europe-west1-{GCP_PROJECT_ID}.cloudfunctions.net/{function_name}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


def get_dependencies(tables_config):
    # 1. DAGS dependencies
    dags_list_of_list = [
        job_params.get("dag_depends", "") for _, job_params in tables_config.items()
    ]
    dags = list(set([dag for dag_list in dags_list_of_list for dag in dag_list]))
    dag_dependencies = []
    # initialize dict
    for dag in dags:
        d = {"dependency_type": "dag", "upstream_id": dag, "dependant_tables": []}
        for table, job_params in tables_config.items():
            for dag in job_params.get("dag_depends", ""):
                if dag in d["upstream_id"]:
                    d["dependant_tables"].append(table)

        dag_dependencies.append(d)

    # 2. TABLES dependencies
    tables_list_of_list = [
        job_params.get("depends", "") for _, job_params in tables_config.items()
    ]
    upstream_tables = list(
        set([table for table_list in tables_list_of_list for table in table_list])
    )
    tables_dependencies = []
    # initialize dict
    for upstream_table in upstream_tables:
        t = {
            "dependency_type": "table",
            "upstream_id": upstream_table,
            "dependant_tables": [],
        }
        for table, job_params in tables_config.items():
            for dependant_table in job_params.get("depends", ""):
                if dependant_table in t["upstream_id"]:
                    t["dependant_tables"].append(table)

        tables_dependencies.append(t)

    dependencies = dag_dependencies + tables_dependencies

    return dependencies


def waiting_operator(
    dag,
    dag_id,
    external_task_id="end",
    allowed_states=["success"],
    failed_states=["failed", "upstream_failed", "skipped"],
):
    """
    A simple dependency operator.

    Creates an ExternalTaskSensor that pauses the current DAG
    until a specific task in another DAG finishes (default "end"). This assumes
    that both DAGs are scheduled in lockstep (e.g., both run daily
    at midnight), so the sensor can directly wait for the run with
    the same execution date.

    Use this when DAG schedules are perfectly aligned.
    """
    return ExternalTaskSensor(
        task_id=f"wait_for_{dag_id}_{external_task_id}",
        external_dag_id=dag_id,
        external_task_id=external_task_id,
        check_existence=True,
        mode="reschedule",
        allowed_states=allowed_states,
        failed_states=failed_states,
        email_on_retry=False,
        dag=dag,
    )


def delayed_waiting_operator(
    dag,
    external_dag_id: str,
    external_task_id: str = "end",
    allowed_states: list = ["success"],
    failed_states: list = ["failed", "upstream_failed", "skipped"],
    offset_days: int = 0,
    window_days: int = 1,
    offset_hours: int = 0,
    window_hours: int = 0,
    skip_manually_triggered: bool = False,
    pool: str = "default_pool",
    **kwargs,
):
    """
    Creates a cross-DAG dependency sensor with a flexible time window.

    Purpose:
        - Waits for a task in another DAG to finish.
        - Handles DAGs with misaligned schedules or different execution intervals.
        - Skips waiting for manual DAG runs if `skip_manually_triggered=True`.

    Window logic:
        - upper_bound = DAG2 logical_date - offset_days/hours
        - lower_bound = upper_bound - window_days/hours
        This ensures we wait for the latest valid run of the external DAG
        **before** DAG2's current logical_date, optionally shifted by offsets.

        ASCII example:
            DAG2 logical_date = 2025-09-12 01:00
            offset_days=0, window_days=1
            -------------------------------
            |  2025-09-11 01:00  |  upper_bound = 2025-09-12 01:00
            |  2025-09-11 00:00  |  lower_bound = upper_bound - window
            -------------------------------
            External DAG runs between lower and upper bounds will be considered.

    Guarantees:
        - Only scheduled DAG runs are considered.
        - Manual triggers are skipped if `skip_manually_triggered=True`.
        - The sensor always waits for the latest external DAG run in the defined window.

    Args:
        dag: The DAG where this task will be added.
        external_dag_id (str): ID of the external DAG.
        external_task_id (str): Task ID in the external DAG to wait for.
        allowed_states (list): States considered successful. Defaults to ["success"].
        failed_states (list): States considered failed. Defaults to ["failed", "upstream_failed", "skipped"].
        offset_days (int): Shift the window back in days.
        window_days (int): Width of the window in days.
        offset_hours (int): Shift the window back in hours.
        window_hours (int): Width of the window in hours.
        skip_manually_triggered (bool): Skip waiting for manual DAG runs if True.
        pool (str): Airflow pool to use.
        **kwargs: Additional arguments passed to the ExternalTaskSensor or PythonOperator.
    """
    if allowed_states is None:
        allowed_states = ["success"]
    if failed_states is None:
        failed_states = ["failed", "upstream_failed", "skipped"]

    task_id = f"wait_for_{external_dag_id}_{external_task_id}"

    def compute_execution_date_fn(logical_date: datetime, **context):
        """
        Determine the external DAG run to wait for based on a flexible window.

        - Aligns with the current DAG run (logical_date).
        - Calculates a window using offset_days/hours and window_days/hours.
        - Returns the most recent external DAG run within this window.

        This allows robust cross-DAG dependencies even when DAG schedules
        differ (e.g., hourly DAG waits for daily DAG).

        Raises:
            ValueError if window parameters are invalid (zero or negative).
        """
        if logical_date is None:
            raise ValueError("The 'logical_date' is missing in the context.")
        if window_days == 0 and window_hours == 0:
            raise ValueError("The window cannot be zero")
        if window_days < 0 or window_hours < 0:
            raise ValueError("The window parameters cannot be negative")
        # Compute upper date limit for external DAG (defaults to logical_date minus offsets)
        upper_bound = logical_date - timedelta(days=offset_days, hours=offset_hours)
        # Compute lower date limit (defaults to window_days=1 day before DAG's logical_date)
        lower_bound = upper_bound - timedelta(days=window_days, hours=window_hours)

        logging.info(
            "Looking for external DAG '%s' runs between lower=%s and upper=%s",
            external_dag_id,
            lower_bound,
            upper_bound,
        )

        return get_last_execution_date(
            external_dag_id,
            lower_date_limit=lower_bound,
            upper_date_limit=upper_bound,
        )

    # If manual triggers should be skipped, create a PythonOperator instead
    if skip_manually_triggered:

        def handle_manual_trigger(**context):
            """
            Skip waiting for manually triggered DAG runs.

            - Manual DAG runs are typically ad-hoc or test runs.
            - Scheduled runs still enforce the cross-DAG dependency.
            - Creates a sensor dynamically at runtime and checks external task status.
            """
            dag_run = context.get("dag_run")
            if dag_run and dag_run.run_id.startswith("manual__"):
                logging.info(f"Skipping wait for manual trigger: {dag_run.run_id}")
                return None  # Skip waiting

            logging.info(
                f"Waiting for external task {external_dag_id}.{external_task_id}"
            )

            # Create a sensor dynamically at runtime
            sensor = ExternalTaskSensor(
                task_id=task_id,
                external_dag_id=external_dag_id,
                external_task_id=external_task_id,
                execution_date_fn=compute_execution_date_fn,
                allowed_states=allowed_states,
                failed_states=failed_states,
                dag=None,  # Important: don't attach to DAG since this is runtime execution
            )
            return sensor.poke(
                context=context
            )  # Check external task status dynamically

        return PythonOperator(
            task_id=task_id,
            python_callable=handle_manual_trigger,
            dag=dag,
            pool=pool,
        )

    # If manual triggers are NOT skipped, return the normal ExternalTaskSensor
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        execution_date_fn=compute_execution_date_fn,
        allowed_states=allowed_states,
        failed_states=failed_states,
        check_existence=True,
        mode="reschedule",
        dag=dag,
        pool=pool,
        **kwargs,
    )


def depends_loop(
    tables_config, jobs: dict, default_upstream_operator, dag, default_end_operator
):
    default_downstream_operators = []
    has_downstream_dependencies = []

    dependencies = get_dependencies(tables_config)

    tables_with_dependencies = [
        dependency["dependant_tables"] for dependency in dependencies
    ]
    tables_with_dependencies = list(
        set([table for table_list in tables_with_dependencies for table in table_list])
    )  # flatten list

    for table, jobs_def in jobs.items():
        operator = jobs_def["operator"]
        # Case for tables without dependencies
        if table not in tables_with_dependencies:
            default_downstream_operators.append(operator)
            operator.set_upstream(default_upstream_operator)

    for dependency in dependencies:
        if dependency["dependency_type"] == "dag":
            if "/" in dependency["upstream_id"]:
                depend_job = waiting_operator(
                    dag,
                    dependency["upstream_id"].split("/")[0],
                    external_task_id=dependency["upstream_id"].split("/")[-1],
                )
            else:
                depend_job = waiting_operator(
                    dag, dependency["upstream_id"], external_task_id="end"
                )

            # get all dependant tasks
            dependant_tables = dependency["dependant_tables"]
            dependant_tasks = [
                jobs_params["operator"]
                for table, jobs_params in jobs.items()
                if table in dependant_tables
            ]
            for dependant_task in dependant_tasks:
                has_downstream_dependencies.append(dependant_task)
                dependant_task.set_upstream(depend_job)

            depend_job.set_upstream(default_upstream_operator)

        elif dependency["dependency_type"] == "table":
            depend_job = [
                jobs_params["operator"]
                for table, jobs_params in jobs.items()
                if table == dependency["upstream_id"]
            ][0]

            dependant_tables = dependency["dependant_tables"]

            dependant_tasks = [
                jobs_params["operator"]
                for table, jobs_params in jobs.items()
                if table in dependant_tables
            ]

            for dependant_task in dependant_tasks:
                if dependant_task not in has_downstream_dependencies:
                    has_downstream_dependencies.append(dependant_task)

                dependant_task.set_upstream(depend_job)

    if len(has_downstream_dependencies) > 0:
        default_end_operator.set_upstream(has_downstream_dependencies)
    else:
        default_end_operator.set_upstream(default_downstream_operators)

    return [
        x for x in default_downstream_operators if x not in has_downstream_dependencies
    ]


def from_external(conn_id, sql_path):
    include = "{% include '" + sql_path + "' %}"
    return f' SELECT * FROM EXTERNAL_QUERY("{conn_id}", """ {include} """ ) ; '


def one_line_query(sql_path):
    with open(f"{sql_path}", "r") as fp:
        lines = " ".join([line.strip() for line in fp.readlines()])
    return lines


def get_airflow_schedule(schedule_interval, local_env=LOCAL_ENV):
    if local_env == "1":
        return None
    else:
        return schedule_interval


def decode_output(task_id, key, **kwargs):
    ti = kwargs["ti"]
    output = ti.xcom_pull(task_ids=task_id, key=key)
    decoded_output = output.decode("utf-8")

    return decoded_output


def get_tables_config_dict(PATH, BQ_DATASET, is_source=False, dbt_alias=False):
    """
    Generates a dictionary configuration for tables based on SQL files in a given directory.

    Args:
        PATH (str): The path to the directory containing SQL files.
        BQ_DATASET (str): The BigQuery dataset where the tables will be stored or loaded from.
        is_source (bool): If True, use 'source_dataset' and 'source_table' keys instead of 'destination_dataset' and 'destination_table'.
    Returns:
        dict: A dictionary where each key is a table name (derived from the SQL file name) and each value is a dictionary containing:
            - 'sql': The path to the SQL file.
            - 'source_dataset' or 'destination_dataset': The BigQuery dataset.
            - 'source_table' or 'destination_table': The name of the table in BigQuery.
    """
    tables_config = {}
    for file in os.listdir(PATH):
        extension = file.split(".")[-1]
        table_name = file.split(".")[0]
        if extension == "sql":
            tables_config[table_name] = {}
            tables_config[table_name]["sql"] = os.path.join(PATH, file)
            if is_source:
                tables_config[table_name]["source_dataset"] = BQ_DATASET
                tables_config[table_name]["source_table"] = (
                    f"applicative_database_{table_name}"
                )
            else:
                tables_config[table_name]["destination_dataset"] = BQ_DATASET
                tables_config[table_name]["destination_table"] = (
                    f"applicative_database_{table_name}"
                )
    for table_name, table_config in tables_config.items():
        if dbt_alias:
            table_config["table_alias"] = table_name.split("__")[-1]
    return tables_config


def sparkql_health_check(url: str, timeout=5, retries=5, initial_delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, params={"query": "ASK { }"}, timeout=timeout)
            logging.info(f"response: {response}")
            if response.status_code == 200:
                logging.info(f"{url} is healthy on attempt {attempt + 1}.")
                return True  # Exit on successful health check
            else:
                logging.warning(
                    f"{url} returned status code {response.status_code} on attempt {attempt + 1}."
                )

        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt + 1}: Could not reach {url}. Error: {e}")

        if attempt < retries - 1:
            delay = initial_delay * (2**attempt)
            logging.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    raise Exception(f"Health check failed for {url} after {retries} attempts.")


def get_json_from_gcs(bucket_name: str, blob_name: str) -> dict:
    """
    Fetch a JSON file from Google Cloud Storage.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return json.loads(blob.download_as_text())
    except NotFound as e:
        logging.error(f"Error fetching JSON from GCS: {str(e)}")
        return {}


def save_json_to_gcs(data_dict, bucket_name, blob_name):
    """
    Upload a dictionary as JSON to Google Cloud Storage.

    Args:
        data_dict (dict): The dictionary to be uploaded
        bucket_name (str): Name of the GCS bucket
        blob_name (str): Path/name of the blob in the bucket
    """
    try:
        # Initialize the client
        client = storage.Client()

        # Get bucket
        bucket = client.bucket(bucket_name)

        # Get blob
        blob = bucket.blob(blob_name)

        # Convert dict to JSON string
        json_data = json.dumps(data_dict, indent=2)  # Added indent for readability

        # Upload to GCS with explicit encoding
        blob.upload_from_string(
            data=json_data,
            content_type="application/json",
            num_retries=3,  # Add retries
            timeout=120,  # Add timeout in seconds
        )
        print(f"Successfully uploaded to gs://{bucket_name}/{blob_name}")

    except Exception as e:
        print(f"Error uploading to GCS: {str(e)}")
        raise


def build_export_context(**kwargs):
    """
    SINGLE FUNCTION that handles:
        1) Gathering export info from GCS & Secret Manager.
        2) Generating export logs & encryption key.
        3) Generating export configurations dynamically.
        4) Pushing ALL relevant fields to XCom, including 'export_context' & 'export_configs'.
    """
    ti = kwargs["ti"]
    partner_name = kwargs["partner_name"]
    logs_bucket = kwargs["bucket"]
    export_date = kwargs["export_date"]
    parquet_storage_gcs_bucket = kwargs["parquet_storage_gcs_bucket"]

    # 1) Fetch next_exported_tables.json, obfuscation_config from GCS
    table_list = get_json_from_gcs(
        logs_bucket, f"{partner_name}/next_exported_tables.json"
    ).get("exported_tables", [])

    obfuscation_config = (
        get_json_from_gcs(logs_bucket, f"{partner_name}/next_obfuscation_config.json")
        or {}
    )

    partner_salt = create_key_if_not_exists(
        project_id=GCP_PROJECT_ID,
        secret_id=f"dbt_export_private_obfuscation_salt_{partner_name}",
        key_length=32,
    )

    # 4) Generate export logs
    export_logs = {
        "partner_name": partner_name,
        "export_date": export_date,
        "exported_tables": table_list,
        "obfuscation_config": obfuscation_config,
    }

    # Save logs to GCS if needed
    save_json_to_gcs(
        export_logs,
        logs_bucket,
        f"{partner_name}/{export_date}/export_logs.json",
    )

    # 5) Build export context for BigQuery to GCS export
    export_context = {
        "table_list": table_list,
        "project_id": GCP_PROJECT_ID,
        "source_dataset": f"tmp_export_{partner_name}",
        "destination_bucket": parquet_storage_gcs_bucket,
        "destination_path": f"{partner_name}/{export_date}",
    }
    ti.xcom_push(key="export_context", value=export_context)

    # 6) Generate dynamic export configurations
    export_configs = []
    for table_name in table_list:
        config = {
            "source_project_dataset_table": f"{GCP_PROJECT_ID}.{export_context['source_dataset']}.{table_name}",
            "destination_cloud_storage_uris": [
                f"gs://{export_context['destination_bucket']}/{export_context['destination_path']}/{table_name}/*.parquet"
            ],
        }
        export_configs.append(config)
        logging.info(f"Generated export config for table {table_name}: {config}")

    if not export_configs:
        raise ValueError("No export configurations were generated")

    # Push required values to XCom
    ti.xcom_push(key="partner_name", value=partner_name)
    ti.xcom_push(key="export_date", value=export_date)
    ti.xcom_push(key="partner_salt", value=partner_salt)
    ti.xcom_push(key="obfuscation_config", value=obfuscation_config)
    ti.xcom_push(key="table_list", value=table_list)

    logging.info(f"[build_export_context] for {partner_name}: {export_context}")
    logging.info(f"Generated {len(export_configs)} export configurations")
    return export_configs
