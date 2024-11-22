import copy
import datetime

from common import macros
from common.alerts import task_fail_slack_alert
from common.config import DAG_FOLDER, GCP_PROJECT_ID
from common.operators.bigquery import bigquery_job_task
from common.utils import get_airflow_schedule
from dependencies.firebase.import_firebase import import_tables

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def validate_string_or_list(value):
    """Validate that the input is either a string or a list of strings."""
    if isinstance(value, str):
        return value
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        if len(value) < 2:
            raise ValueError("custom_day list should contain at least two dates.")
        return value
    else:
        raise ValueError("Value must be a string or a list of strings.")


def get_first_and_last_days(custom_day):
    """Extract the first and last days from a list of dates in YYYYMMDD format."""
    if isinstance(custom_day, list):
        first_day = f"{{{{ yyyymmdd('{custom_day[0]}') }}}}"
        last_day = f"{{{{ yyyymmdd('{custom_day[-1]}') }}}}"
        return first_day, last_day
    else:
        raise ValueError("custom_day should be a list of dates in YYYY-MM-DD format.")


dags = {
    "daily": {
        "prefix": "_",
        "schedule_interval": "00 13 * * *",
        # two days ago
        "yyyymmdd": "{{ yyyymmdd(add_days(ds, -1)) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 6,
            "retry_delay": datetime.timedelta(hours=6),
            "project_id": GCP_PROJECT_ID,
        },
    },
    "intraday": {
        "prefix": "_intraday_",
        "schedule_interval": "00 01 * * *",
        # one day ago
        "yyyymmdd": "{{ yyyymmdd(ds) }}",
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 3,
            "retry_delay": datetime.timedelta(minutes=30),
            "project_id": GCP_PROJECT_ID,
        },
    },
    "specific_day": {
        "prefix": "_",
        "schedule_interval": None,
        "default_dag_args": {
            "start_date": datetime.datetime(2022, 6, 9),
            "retries": 3,
            "retry_delay": datetime.timedelta(minutes=30),
            "project_id": GCP_PROJECT_ID,
        },
        "input_params": {
            "custom_day": Param(
                default=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ),  # yesterday
                type=validate_string_or_list,
                description="Enter a date in YYYY-MM-DD format (string) or a list of (string) dates",
            ),
            "target": Param(
                default="both",
                type=["native", "pro", "both"],
                description="Specify the target type: 'native', 'pro', or 'both'.",
            ),
        },
    },
}


def check_table_exists(**kwargs):
    import logging

    job_params = kwargs["job_params"]
    job_name = kwargs["job_name"]
    table = kwargs["table"]
    bq_hook = BigQueryHook()
    client = bq_hook.get_client(project_id=GCP_PROJECT_ID)
    for dataset_id in job_params.get("gcp_project_env", []):
        logging.info(f"Check if {table} exists in {dataset_id}")
        try:
            client.get_table(f"{dataset_id}.{table}")
        except Exception:
            logging.info(f"Not found {table} in {dataset_id}, error. Fallback.")
            return f"fallback_{job_name}"
    logging.info(f"Everything is fine, proceed with default_{job_name}")
    return f"default_{job_name}"


for dag_type, params in dags.items():
    dag_id = f"import_{dag_type}_firebase_data"
    prefix = params["prefix"]
    yyyymmdd = params.get("yyyymmdd")

    if dag_type == "specific_day":
        if isinstance(params["input_params"]["custom_day"], list):
            first_day, last_day = get_first_and_last_days(
                params["input_params"]["custom_day"]
            )
            yyyymmdd = f"{first_day}to{last_day}"
        else:
            yyyymmdd = "{{ yyyymmdd(params.custom_day) }}"

    dag = DAG(
        dag_id,
        default_args=params["default_dag_args"],
        description="Import firebase data and dispatch it to each env",
        on_failure_callback=task_fail_slack_alert,
        schedule_interval=get_airflow_schedule(params["schedule_interval"]),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=90),
        user_defined_macros=macros.default,
        template_searchpath=DAG_FOLDER,
        params=params.get("input_params", {}),
        render_template_as_native_obj=True,
    )
    if dag_type == "specific_day":
        yyyymmdd = "{{ yyyymmdd(params.custom_day) }}"

    globals()[dag_id] = dag

    start = DummyOperator(task_id="start", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    import_tables_temp = copy.deepcopy(import_tables)
    target = params.get("input_params", {}).get("target", "both")
    for job_name_table, job_params in import_tables_temp.items():
        if (
            target != "both" and target not in job_name_table
        ) or dag_type != "specific_day":
            continue
        if dag_type == "specific_day" and isinstance(
            params["input_params"]["custom_day"].default, list
        ):
            first_day, last_day = get_first_and_last_days(
                params["input_params"]["custom_day"].default
            )
            table_id = f"events{prefix}{first_day}to{last_day}"
        elif dag_type == "specific_day":
            table_id = f"events{prefix}" + "{{ yyyymmdd(params.custom_day) }}"
        elif dag_type == "intraday":
            table_id = f"events{prefix}" + "{{ yyyymmdd(ds) }}"
        elif dag_type == "daily":
            table_id = f"events{prefix}" + "{{ yyyymmdd(add_days(ds, -1)) }}"

        check_table_task = BranchPythonOperator(
            dag=dag,
            task_id=f"check_table_exists_task_{job_name_table}",
            python_callable=check_table_exists,
            provide_context=True,
            op_kwargs={
                "job_params": job_params["params"],
                "table": table_id,
                "job_name": job_name_table,
            },
        )

        default_task = bigquery_job_task(
            dag=dag,
            table=f"default_{job_name_table}",
            job_params=job_params,
            extra_params={"prefix": prefix, "dag_type": dag_type},
        )

        fallback_task = bigquery_job_task(
            dag=dag,
            table=f"fallback_{job_name_table}",
            # overwrite default configuration, execute only if the default task fails
            job_params=job_params,
            extra_params=dict(
                {"prefix": prefix, "dag_type": dag_type},
                **job_params.get("fallback_params", {}),
            ),
        )

        end_job = DummyOperator(
            task_id=f"end_{job_name_table}", dag=dag, trigger_rule="one_success"
        )

        start >> check_table_task >> [default_task, fallback_task] >> end_job >> end
