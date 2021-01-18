from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

# Declare variables
# Airflow
client_id = "837036835383-uo5mps16sfk4s2pf1h1v7papnsk351al.apps.googleusercontent.com"
webserver_id = "q775b71be829eada6p-tp"
dag_name = "restore_prod_from_vm_export_v1"

# Compute Engine Instance
project = "pass-culture-app-projet-test"
zone = "europe-west1-b"
instance = "data-dump-scalingo"


def trigger_dag(data, context=None):
    webserver_url = (
        f"https://{webserver_id}.appspot.com/api/experimental/dags/{dag_name}/dag_runs"
    )
    stop_compute_engine()
    make_iap_request(webserver_url, client_id, method="POST", json={"conf": data})


def make_iap_request(url, client_id, method="GET", **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    resp = requests.request(
        method,
        url,
        headers={"Authorization": "Bearer {}".format(google_open_id_connect_token)},
        **kwargs,
    )
    if resp.status_code == 403:
        raise Exception(
            "Service account does not have permission to "
            "access the IAP-protected application."
        )
    elif resp.status_code != 200:
        raise Exception(
            "Bad response from application: {!r} / {!r} / {!r}".format(
                resp.status_code, resp.headers, resp.text
            )
        )
    else:
        return resp.text


def stop_compute_engine():
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build("compute", "v1", credentials=credentials)
    request = service.instances().stop(project=project, zone=zone, instance=instance)
    response = request.execute()
    pprint(response)
