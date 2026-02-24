import logging

from google.cloud import bigquery, secretmanager, storage

from constants import BQ_LOCATION, SECRET_VERSION

logger = logging.getLogger(__name__)


### SECRET MANAGEMENT
def get_secret(project_id: str, secret_id: str, version: str = SECRET_VERSION) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8").strip()


### GCS
def upload_to_gcs(
    bucket_name: str,
    blob_name: str,
    content: bytes,
    content_type: str,
    gcs_client: storage.Client | None = None,
) -> str:
    client = gcs_client or storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_string(content, content_type=content_type)
    return f"gs://{bucket_name}/{blob_name}"


### BIGQUERY
def get_bq_client(project_id: str) -> bigquery.Client:
    return bigquery.Client(project=project_id, location=BQ_LOCATION)
