"""Configuration management for the Metabase migration tool.

Loads settings from environment variables and GCP Secret Manager.
"""

from __future__ import annotations

import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

# --- Environment variables ---

PROJECT_NAME: str = os.environ.get("PROJECT_NAME", "")
ENV_SHORT_NAME: str = os.environ.get("ENV_SHORT_NAME", "dev")

_ENV_LONG_NAMES: dict[str, str] = {
    "dev": "development",
    "stg": "staging",
    "prod": "production",
}
ENV_LONG_NAME: str = _ENV_LONG_NAMES.get(ENV_SHORT_NAME, "development")

INT_METABASE_DATASET: str = f"int_metabase_{ENV_SHORT_NAME}"
METABASE_API_USERNAME: str = os.environ.get(
    "METABASE_API_USERNAME", "metabase-data-bot@passculture.app"
)


def access_secret(
    project_id: str,
    secret_id: str,
    default: str | None = None,
) -> str | None:
    """Fetch a secret from GCP Secret Manager.

    Args:
        project_id: GCP project ID.
        secret_id: Secret name in Secret Manager.
        default: Value to return if credentials are unavailable (local dev).

    Returns:
        The secret payload as a string, or `default` if unavailable.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def get_metabase_host() -> str:
    """Get the Metabase host URL from Secret Manager or env var."""
    env_host = os.environ.get("METABASE_HOST")
    if env_host:
        return env_host
    host = access_secret(PROJECT_NAME, f"metabase_host_{ENV_LONG_NAME}")
    if not host:
        raise ValueError("METABASE_HOST not set and secret not available")
    return host


def get_metabase_client_id() -> str:
    """Get the OAuth2 client ID for Metabase."""
    env_val = os.environ.get("METABASE_CLIENT_ID")
    if env_val:
        return env_val
    val = access_secret(PROJECT_NAME, f"metabase-{ENV_LONG_NAME}_oauth2_client_id")
    if not val:
        raise ValueError("METABASE_CLIENT_ID not set and secret not available")
    return val


def get_metabase_password() -> str:
    """Get the Metabase API password from Secret Manager or env var."""
    env_val = os.environ.get("METABASE_PASSWORD")
    if env_val:
        return env_val
    val = access_secret(PROJECT_NAME, f"metabase-api-secret-{ENV_SHORT_NAME}")
    if not val:
        raise ValueError("METABASE_PASSWORD not set and secret not available")
    return val
