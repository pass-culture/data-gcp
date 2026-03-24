import json
import os

import mlflow
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from constants import MLFLOW_SECRET_NAME, MLFLOW_URI, SA_ACCOUNT
from utils.secrets_utils import get_secret


def connect_remote_mlflow() -> None:
    service_account_dict = json.loads(get_secret(SA_ACCOUNT))
    mlflow_client_audience = get_secret(MLFLOW_SECRET_NAME)

    id_token_credentials = service_account.IDTokenCredentials.from_service_account_info(
        service_account_dict, target_audience=mlflow_client_audience
    )
    id_token_credentials.refresh(Request())

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token_credentials.token
    mlflow.set_tracking_uri(MLFLOW_URI)
