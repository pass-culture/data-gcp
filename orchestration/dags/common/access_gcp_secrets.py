from google.auth.exceptions import DefaultCredentialsError
from google.api_core.exceptions import PermissionDenied, NotFound
from google.cloud import secretmanager


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except (DefaultCredentialsError, PermissionDenied, NotFound) as e:
        return default
