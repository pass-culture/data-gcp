from google.cloud import secretmanager, storage

from src.constants import GCP_PROJECT_ID


def get_last_date_from_bucket(gcs_path: str) -> str:
    """
    Get the last date from the GCS path.

    Args:
        gcs_path (str): The GCS path to be used. (ex: gs://bucket-name/path/to/base/path)
    """
    dates = get_datet_from_bucket(gcs_path=gcs_path)

    if not dates:
        raise ValueError(f"No dates found in bucket {gcs_path}")
    return sorted(dates, reverse=True)[0]


def get_datet_from_bucket(gcs_path: str) -> list[str]:
    """
    Get all dates from the GCS path.

    Args:
        gcs_path (str): The GCS path to be used. (ex: gs://bucket-name/path/to/base/path)
    """
    client = storage.Client()
    storage_path = gcs_path.replace("gs://", "")
    bucket_name = storage_path.split("/")[0]
    base_path = storage_path.split("/", 1)[1]
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=base_path)

    dates = []
    for blob in blobs:
        if len(blob.name.split("/")) > 3:
            raise ValueError(
                f"Invalid blob name {blob.name}. Expected format: {gcs_path}/YYYYMMDD/file"
            )
        elif len(blob.name.split("/")) == 3:
            dates.append(blob.name.split("/")[-2])

    return sorted(set(dates), reverse=True)


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")
