from google.auth.transport.requests import Request
from google.oauth2 import id_token
import mlflow
import os
from pcpapillon.utils.env_vars import MLFLOW_URL


def connect_remote_mlflow(client_id):
    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_URL
    mlflow.set_tracking_uri(uri)
