import functools

from constants import GCP_PROJECT_ID
from google.cloud import secretmanager


@functools.lru_cache
def get_secret(secret_name: str) -> str:
    """Retrieve secret value from Google Secret Manager.

    Args:
        secret_name: Name of the secret to retrieve

    Returns:
        Secret value as a string
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve secret '{secret_name}': {e}")
