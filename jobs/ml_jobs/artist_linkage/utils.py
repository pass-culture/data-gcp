import io
from typing import Tuple
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from pandas import DataFrame


def _parse_gcs_path(gcs_path: str) -> Tuple[str, str]:
    """Parse the GCS path into bucket and blob."""
    if not gcs_path.startswith("gs://"):
        raise ValueError("Path must start with 'gs://'")
    parsed_url = urlparse(gcs_path)
    bucket_name = parsed_url.netloc
    blob_name = parsed_url.path.lstrip("/")
    return bucket_name, blob_name


def upload_parquet(dataframe: DataFrame, gcs_path: str) -> None:
    """Upload a Pandas DataFrame as a Parquet file to the GCS path with a new filename."""
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


def read_parquet(gcs_path: str) -> DataFrame:
    """Read a Parquet file from the GCS path into a Pandas DataFrame."""
    bucket_name, blob_name = _parse_gcs_path(gcs_path)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    file_stream = io.BytesIO()
    blob.download_to_file(file_stream)
    file_stream.seek(0)
    return pq.read_table(file_stream).to_pandas()
