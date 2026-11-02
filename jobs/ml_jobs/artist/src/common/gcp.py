from google.cloud import secretmanager, storage

from src.common.constants import GCP_PROJECT_ID


def _split_gcs_path(gcs_path: str) -> tuple[str, str]:
    """Split gs://bucket-name/path/to/base/path into (bucket-name, path/to/base/path/)."""
    bucket_name, _, base_path = gcs_path.removeprefix("gs://").partition("/")
    return bucket_name, f"{base_path.rstrip('/')}/"


def get_last_date_from_bucket(gcs_path: str, required_file_name: str) -> str:
    """
    Get the last date from the GCS path whose folder contains the required file.

    Args:
        gcs_path (str): The GCS path to be used. (ex: gs://bucket-name/path/to/base/path)
        required_file_name (str): Only date folders containing this file are considered,
            so an incomplete latest folder falls back to the previous one.
    """
    dates = get_dates_from_bucket(gcs_path=gcs_path)
    if not dates:
        raise ValueError(f"No dates found in bucket {gcs_path}")

    bucket_name, base_path = _split_gcs_path(gcs_path)
    bucket = storage.Client().bucket(bucket_name)
    for date in dates:
        if bucket.blob(f"{base_path}{date}/{required_file_name}").exists():
            return date
    raise ValueError(f"No date folder in {gcs_path} contains {required_file_name}")


def get_dates_from_bucket(gcs_path: str) -> list[str]:
    """
    Get all dates from the GCS path, most recent first.

    Only the immediate YYYYMMDD sub-folders are considered: nested content
    (ex: gs://bucket-name/path/YYYYMMDD/raw/file) is ignored.

    Args:
        gcs_path (str): The GCS path to be used. (ex: gs://bucket-name/path/to/base/path)
    """
    bucket_name, base_path = _split_gcs_path(gcs_path)
    blobs = storage.Client().list_blobs(bucket_name, prefix=base_path, delimiter="/")
    # `prefixes` is only populated once the iterator has been consumed
    for _ in blobs:
        pass

    dates = {prefix.removeprefix(base_path).rstrip("/") for prefix in blobs.prefixes}
    return sorted(
        (date for date in dates if len(date) == 8 and date.isdigit()), reverse=True
    )


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")
