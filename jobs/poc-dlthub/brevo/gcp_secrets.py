from google.cloud import secretmanager


def get_brevo_api_key(gcp_project: str, env: str, audience: str) -> str:
    secret_ids = {"native": f"sendinblue-api-key-{env}", "pro": f"sendinblue-pro-api-key-{env}"}

    secret_id = secret_ids[audience]

    return _get_secret(gcp_project, secret_id)


def _get_secret(gcp_project: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{gcp_project}/secrets/{secret_id}/versions/latest"

    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode("UTF-8")
