from google.cloud import storage


def get_last_date_from_bucket(gcs_path: str) -> str:
    """
    Get the last date from the GCS path.

    Args:
        gcs_path (str): The GCS path to be used. (ex: gs://bucket-name/path/to/base/path)
    """
    client = storage.Client()
    storage_path = gcs_path.replace("gs://", "")
    bucket_name = storage_path.split("/")[0]
    base_path = storage_path.replace(f"{bucket_name}/", "")
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

    if not dates:
        raise ValueError(
            f"No dates found in bucket {bucket_name} with base path {base_path}"
        )
    return sorted(dates, reverse=True)[0]
