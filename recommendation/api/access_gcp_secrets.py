from google.cloud import secretmanager


def access_secret(project_id, secret_id, version_id=1):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name)
    return response.payload.data.decode("UTF-8")
