from google.cloud import storage
import os
import logging
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"

def access_secret_data(project_id, secret_id, version_id=2, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
