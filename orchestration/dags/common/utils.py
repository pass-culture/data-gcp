from google.auth.transport.requests import Request
from google.oauth2 import id_token
from common.config import (
    GCP_PROJECT,
)


def getting_service_account_token(function_name):
    function_url = (
        f"https://europe-west1-{GCP_PROJECT}.cloudfunctions.net/{function_name}"
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
