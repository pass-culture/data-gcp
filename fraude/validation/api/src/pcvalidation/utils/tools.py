from google.auth.transport.requests import Request
from google.oauth2 import id_token
import mlflow
import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from pcvalidation.utils.env_vars import MLFLOW_URL

def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default

def connect_remote_mlflow(client_id):
    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_URL
    mlflow.set_tracking_uri(uri)