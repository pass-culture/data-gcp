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


def waiting_operator(dag, dag_id, task_id):

    hasher = hashlib.sha1(task_id.encode("utf-8"))
    hashed_dag_id = (
        str(base64.urlsafe_b64encode(hasher.digest()[:10]))
        .replace("=", "")
        .replace("'", "")
    )

    return ExternalTaskSensor(
        task_id=f"wait_for_{dag_id}_{hashed_dag_id}",
        external_dag_id=dag_id,
        external_task_id="end",
        check_existence=True,
        mode="reschedule",
        allowed_states=ALLOWED_STATES,
        failed_states=FAILED_STATES,
        email_on_retry=False,
        dag=dag,
    )


def depends_loop(jobs: dict, default_upstream_operator, dag):
    default_downstream_operators = []
    has_downstream_dependencies = []

    table_dependencies = [
        {
            "dependency_type": "table",
            "task_id": table,
            "task": jobs_def["operator"],
            "depends_on": dependency,
        }
        for table, jobs_def in jobs.items()
        for dependency in jobs_def.get("depends", [])
    ]

    dag_dependencies = [
        {
            "dependency_type": "dag",
            "task_id": table,
            "task": jobs_def["operator"],
            "depends_on": dependency,
        }
        for table, jobs_def in jobs.items()
        for dependency in jobs_def.get("dag_depends", [])
    ]

    dependencies = table_dependencies + dag_dependencies

    for table, jobs_def in jobs.items():

        operator = jobs_def["operator"]
        default_downstream_operators.append(operator)
        operator.set_upstream(default_upstream_operator)

        # keep dependencies of the current table only
        for dependency in [
            dependency_task
            for dependency_task in dependencies
            if dependency_task["task_id"] == table
        ]:

            if dependency["dependency_type"] == "dag":
                depend_job = waiting_operator(
                    dag, dependency["depends_on"], task_id=dependency["task_id"]
                )
                has_downstream_dependencies.append(depend_job)
                dependency["task"].set_upstream(depend_job)
                depend_job.set_upstream(default_upstream_operator)

            if dependency["dependency_type"] == "table":
                depend_job = jobs[dependency["depends_on"]]["operator"]
                has_downstream_dependencies.append(depend_job)
                dependency["task"].set_upstream(depend_job)

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