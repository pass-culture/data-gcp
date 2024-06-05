import io
from typing import Tuple
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from pandas import DataFrame


def _parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """
    Parse the GCS path into bucket and blob.

    Args:
        gcs_path (str): The GCS path to be parsed.

    Returns:
        Tuple[str, str]: A tuple containing the bucket name and blob name.

    Raises:
        ValueError: If the path does not start with 'gs://'.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError("Path must start with 'gs://'")
    parsed_url = urlparse(gcs_path)
    bucket_name = parsed_url.netloc
    blob_name = parsed_url.path.lstrip("/")
    return bucket_name, blob_name


def upload_parquet(dataframe: DataFrame, gcs_path: str) -> None:
    """
    Uploads a Pandas DataFrame as a Parquet file to the GCS path with a new filename.

    Args:
        dataframe (DataFrame): The Pandas DataFrame to be uploaded.
        gcs_path (str): The GCS path where the Parquet file will be uploaded.

    Returns:
        None
    """
    bucket_name, blob_name = _parse_gcs_path(gcs_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Convert the DataFrame to a Parquet file in memory
    file_stream = io.BytesIO()
    table = pa.Table.from_pandas(dataframe)
    pq.write_table(table, file_stream)
    file_stream.seek(0)

    # Upload the in-memory Parquet file to GCS
    blob.upload_from_file(file_stream, content_type="application/octet-stream")
    print(f"DataFrame uploaded as Parquet file to gs://{bucket_name}/{blob_name}.")
