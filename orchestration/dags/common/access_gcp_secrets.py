from google.api_core.exceptions import NotFound, PermissionDenied
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": name})
        except TypeError:
            response = client.access_secret_version(name)
        return response.payload.data.decode("UTF-8")
    except (DefaultCredentialsError, PermissionDenied, NotFound):
        return default
