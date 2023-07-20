from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
demarches_jeunes = [47380, 47480]
demarches_pro = [50362, 55475, 57081, 57189, 61589, 62703, 65028]


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
