"""Configuration management for the Metabase migration tool.

Loads settings from environment variables and GCP Secret Manager.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import google.auth
import google.auth.transport.requests
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

logger = logging.getLogger(__name__)

# --- Environment variables ---

PROJECT_NAME: str = os.environ.get("PROJECT_NAME", "")
ENV_SHORT_NAME: str = os.environ.get("ENV_SHORT_NAME", "dev")

_ENV_LONG_NAMES: dict[str, str] = {"dev": "development", "stg": "staging", "prod": "production"}
ENV_LONG_NAME: str = _ENV_LONG_NAMES.get(ENV_SHORT_NAME, "development")

INT_METABASE_DATASET: str = f"int_metabase_{ENV_SHORT_NAME}"
METABASE_API_USERNAME: str = os.environ.get("METABASE_API_USERNAME", "metabase-data-bot@passculture.app")
SA_KEY_PATH: str = os.environ.get(
    "GOOGLE_SA_KEY_PATH", str(Path(__file__).parent / "secrets" / f"algo-training-{ENV_SHORT_NAME}-secret.json")
)


def access_secret(project_id: str, secret_id: str, default: str | None = None) -> str | None:
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


def get_iap_bearer_token() -> str | None:
    """Get an IAP bearer token for the staging/prod Metabase.

    Prefers an explicit service-account JSON key file (``SA_KEY_PATH``) to
    mint an ID token with the correct IAP audience.  When no key file is
    present (e.g. on GCE VMs), falls back to Application Default Credentials.

    Returns None when IAP is not needed:
    - Local dev environments (``ENV_SHORT_NAME == "dev"``)
    - When ``METABASE_HOST`` is explicitly set (e.g. kubectl port-forward
      to bypass IAP: ``METABASE_HOST=http://localhost:8080``)

    Returns:
        A bearer token string, or None if IAP is not needed.
    """
    if ENV_SHORT_NAME == "dev":
        return None

    # When METABASE_HOST is explicitly overridden, assume the caller is
    # bypassing IAP (e.g. via kubectl port-forward) and skip token minting.
    if os.environ.get("METABASE_HOST"):
        logger.info(
            "METABASE_HOST is set (%s) — skipping IAP token (direct access assumed)", os.environ["METABASE_HOST"]
        )
        return None

    client_id = get_metabase_client_id()
    sa_key_path = Path(SA_KEY_PATH)

    # Prefer explicit SA key file (local developer workflow)
    if sa_key_path.is_file():
        from google.oauth2 import service_account as sa_module

        id_token_creds = sa_module.IDTokenCredentials.from_service_account_file(
            str(sa_key_path), target_audience=client_id
        )
        request = google.auth.transport.requests.Request()
        id_token_creds.refresh(request)
        logger.info("Obtained IAP bearer token from SA key file: %s", sa_key_path.name)
        return str(id_token_creds.token)

    # Fallback: use Application Default Credentials (GCE VMs, etc.)
    credentials, _ = google.auth.default()
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)

    if hasattr(credentials, "id_token") and credentials.id_token:
        logger.info("Obtained IAP bearer token from user credentials")
        return str(credentials.id_token)

    from google.oauth2 import id_token

    token: str = id_token.fetch_id_token(request, client_id)
    logger.info("Obtained IAP bearer token for audience %s...", client_id[:20])
    return token
