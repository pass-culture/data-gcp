from google.auth.transport.requests import Request
from google.oauth2 import id_token
from common.config import GCP_PROJECT_ID, MLFLOW_URL, ENV_SHORT_NAME


def getting_service_account_token(function_name):
    function_url = (
        f"https://europe-west1-{GCP_PROJECT_ID}.cloudfunctions.net/{function_name}"
    )
    open_id_connect_token = id_token.fetch_id_token(Request(), function_url)
    return open_id_connect_token


def depends_loop(jobs: dict, default_upstream_operator):
    default_downstream_operators = []
    has_downstream_dependencies = []
    for _, jobs_def in jobs.items():

        operator = jobs_def["operator"]
        dependencies = jobs_def["depends"]
        default_downstream_operators.append(operator)

        if len(dependencies) == 0:
            operator.set_upstream(default_upstream_operator)
        for d in dependencies:
            depend_job = jobs[d]["operator"]
            has_downstream_dependencies.append(depend_job)
            operator.set_upstream(depend_job)

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
