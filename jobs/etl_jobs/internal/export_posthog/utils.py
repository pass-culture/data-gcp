import os
from dataclasses import dataclass
from datetime import datetime

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID")


@dataclass
class PostHogEvent:
    event_type: str
    device_id: str
    properties: dict
    timestamp: datetime
    uuid: str
    user_properties: dict


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
