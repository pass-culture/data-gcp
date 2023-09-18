import os
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
import requests

GCP_PROJECT = os.environ.get("PROJECT_NAME")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")
BIGQUERY_CLEAN_DATASET = os.environ.get(
    "BIGQUERY_CLEAN_DATASET", f"clean_{ENV_SHORT_NAME}"
)
BUCKET_NAME = f"data-bucket-{ENV_SHORT_NAME}"


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def get_api_token(consumer_key):
    headers = {
        "Authorization": f"""Basic {consumer_key}""",
    }

    data = {"grant_type": "client_credentials"}

    response = requests.post(
        "https://api.insee.fr/token", headers=headers, data=data, verify=False
    )
    result_token = response.json()

    return result_token["access_token"]
