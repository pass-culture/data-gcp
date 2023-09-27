from google.auth.transport.requests import Request
from google.oauth2 import id_token
from airflow.sensors.external_task import ExternalTaskSensor
import base64
import hashlib

from common.config import (
    GCP_PROJECT_ID,
    MLFLOW_URL,
    ENV_SHORT_NAME,
    FAILED_STATES,
    ALLOWED_STATES,
    LOCAL_ENV,
)


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


def waiting_operator(dag, dag_id, external_task_id="end"):

    return ExternalTaskSensor(
        task_id=f"wait_for_{dag_id}_{external_task_id}",
        external_dag_id=dag_id,
        external_task_id=external_task_id,
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        email_on_retry=False,
        dag=dag,
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
            has_downstream_dependencies.append(depend_job)
            for dependant_task in dependant_tasks:
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
    return (
        f"SELECT * FROM EXTERNAL_QUERY('{conn_id}', "
        + '"'
        + "{% include '"
        + sql_path
        + "' %}"
        + '"'
        + ");"
    )


def one_line_query(sql_path):
    with open(f"{sql_path}", "r") as fp:
        lines = " ".join([line.strip() for line in fp.readlines()])
    return lines


def get_airflow_schedule(schedule_interval, local_env=LOCAL_ENV):
    if local_env == "1":
        return None
    else:
        return schedule_interval
