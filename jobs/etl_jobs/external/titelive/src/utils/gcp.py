"""GCP utilities for Secret Manager and storage operations."""

from google.cloud import secretmanager


def access_secret_data(
    project_id: str, secret_id: str, version_id: str = "latest"
) -> str:
    """
    Access secret data from GCP Secret Manager.

    Args:
        project_id: GCP project ID
        secret_id: Secret identifier
        version_id: Version of the secret (default: "latest")

    Returns:
        Decoded secret value as string
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
