from google.auth.transport.requests import Request
from google.oauth2 import id_token
from airflow.sensors.external_task import ExternalTaskSensor
from itertools import zip_longest

from common.config import (
    GCP_PROJECT_ID,
    MLFLOW_URL,
    ENV_SHORT_NAME,
    FAILED_STATES,
    ALLOWED_STATES,
)


def getting_service_account_token(function_name):
    function_url = (
        f"https://europe-west1-{GCP_PROJECT_ID}.cloudfunctions.net/{function_name}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


def waiting_operator(dag, dag_id):
    return ExternalTaskSensor(
        task_id=f"wait_for_{dag_id}",
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
    for _, jobs_def in jobs.items():

        operator = jobs_def["operator"]
        dag_dependencies = jobs_def["dag_depends"]
        dependencies = jobs_def["depends"]
        default_downstream_operators.append(operator)

        if len(dependencies) == 0 and len(dag_dependencies) == 0:
            operator.set_upstream(default_upstream_operator)

        if len(dependencies) > 0 and len(dag_dependencies) == 0:
            for d in dependencies:
                depend_job = jobs[d]["operator"]
                has_downstream_dependencies.append(depend_job)
                operator.set_upstream(depend_job)

        if len(dependencies) == 0 and len(dag_dependencies) > 0:
            for d in dag_dependencies:
                dag_depend_job = waiting_operator(dag, d)
                has_downstream_dependencies.append(dag_depend_job)
                operator.set_upstream(dag_depend_job)
                dag_depend_job.set_upstream(default_upstream_operator)

        if len(dependencies) > 0 and len(dag_dependencies) > 0:
            for dag_d, d in zip_longest(dag_dependencies, dependencies, fillvalue=None):
                if d is not None:
                    depend_job = jobs[d].get("operator", None)
                if dag_d is not None:
                    dag_depend_job = waiting_operator(dag, dag_d)
                has_downstream_dependencies.append(dag_depend_job)
                has_downstream_dependencies.append(depend_job)
                dag_depend_job.set_upstream(depend_job)
                operator.set_upstream(dag_depend_job)

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
