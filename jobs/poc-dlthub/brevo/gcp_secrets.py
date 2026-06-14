import logging
import os

from google.cloud import secretmanager

logger = logging.getLogger(__name__)

# Environment variable names for each audience
_ENV_VAR_NAMES = {
    "native": "BREVO_NATIVE_API_KEY",
    "pro": "BREVO_PRO_API_KEY",
}


def get_brevo_api_key(gcp_project: str, env: str, audience: str) -> str:
    """Get the Brevo API key for the given audience.

    Checks for an environment variable first (BREVO_NATIVE_API_KEY or BREVO_PRO_API_KEY),
    then falls back to GCP Secret Manager.
    """
    env_var_name = _ENV_VAR_NAMES[audience]
    api_key = os.environ.get(env_var_name)

    if api_key:
        logger.info("Using Brevo API key from environment variable %s", env_var_name)
        return api_key

    logger.info("Environment variable %s not set, falling back to Secret Manager", env_var_name)
    secret_ids = {"native": f"sendinblue-api-key-{env}", "pro": f"sendinblue-pro-api-key-{env}"}
    secret_id = secret_ids[audience]

    return _get_secret(gcp_project, secret_id)


def _get_secret(gcp_project: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{gcp_project}/secrets/{secret_id}/versions/latest"

    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode("UTF-8")
