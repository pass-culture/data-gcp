from google.cloud import secretmanager


def access_secret_data(project_id, secret_id, version_id=1):
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = secretmanager.SecretManagerServiceClient().access_secret_version(
        name=name
    )
    return response.payload.data.decode("UTF-8")
