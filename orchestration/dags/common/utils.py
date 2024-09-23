import datetime
import logging

from common.config import (
    GCP_PROJECT_ID,
    LOCAL_ENV,
)
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from airflow.models import DagRun
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.types import DagRunType


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
    external_dag_id,
    external_task_id="end",
    allowed_states=["success"],
    failed_states=["failed", "upstream_failed", "skipped"],
    weekly=False,
    lower_date_limit=None,  # Optional lower bound
    **kwargs,
):
    """
    Function to create an ExternalTaskSensor that waits for a task in another DAG to finish,
    with proper handling of execution dates.
    """

    # This function will be passed to execution_date_fn to compute the execution date dynamically
    def compute_execution_date_fn(logical_date, **context):
        """
        Compute the execution date for the ExternalTaskSensor using Airflow's context.
        Fetch the execution_date from the task's execution context.
        """
        # Fetch execution_date from Airflow's context in Airflow 1.x
        current_execution_date = logical_date

        if current_execution_date is None:
            raise ValueError("The 'logical_date' is missing in the context.")

        # Compute the lower date limit or default to the start of the same day
        if weekly:
            lower_limit = (current_execution_date - datetime.timedelta(days=7)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            lower_limit = lower_date_limit or current_execution_date.replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        # Get the last execution date of the external DAG before the current execution date
        return get_last_execution_date(
            external_dag_id,
            upper_date_limit=current_execution_date,
            lower_date_limit=lower_limit,
        )

    # Create the ExternalTaskSensor
    sensor = ExternalTaskSensor(
        task_id=f"wait_for_{external_dag_id}_{external_task_id}",
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        execution_date_fn=compute_execution_date_fn,  # Pass the date function that uses Airflow's context
        check_existence=True,
        mode="reschedule",
        allowed_states=allowed_states,
        failed_states=failed_states,
        dag=dag,
        **kwargs,
    )

    return sensor


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
