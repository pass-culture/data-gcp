from google.cloud import storage
import pandas as pd
import json
import pickle

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


def upload_to_gcs(gcs_path, source_file_name, file_type):
    """Uploads a file to the bucket.
    Args:
    bucket_name: The ID of your GCS bucket.
    source_file_name: The path to your file to upload.
    destination_blob_name: The ID of your GCS object.
    file_type: The type of the file ('parquet', 'json', 'pkl').

    Returns:
    None
    """
    # Initialize a storage client
    storage_client = storage.Client()
    bucket_name, blob_name = _parse_gcs_path(gcs_path)
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object from the bucket
    blob = bucket.blob(blob_name)

    if file_type == "parquet":
        df = pd.read_parquet(source_file_name)
        # Save the DataFrame to a buffer
        buffer = df.to_parquet()
        blob.upload_from_string(buffer, content_type="application/octet-stream")
    elif file_type == "json":
        with open(source_file_name, "r") as json_file:
            json_data = json.load(json_file)
        blob.upload_from_string(json.dumps(json_data), content_type="application/json")
    elif file_type == "pkl":
        with open(source_file_name, "rb") as pkl_file:
            blob.upload_from_file(pkl_file, content_type="application/octet-stream")
    else:
        raise ValueError("Unsupported file type: {}".format(file_type))

    print(f"File {source_file_name} uploaded to {blob_name}.")


# Example usage:
# upload_to_gcs('your-bucket-name', 'local/path/to/file.parquet', 'remote/path/to/file.parquet', 'parquet')

from google.cloud import storage
import pandas as pd


def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket.

    Args:
    bucket_name: The ID of your GCS bucket.
    source_blob_name: The ID of your GCS object.
    destination_file_name: The path to which the file should be downloaded.

    Returns:
    None
    """
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Get the blob
    blob = bucket.blob(source_blob_name)

    # Download the blob to a local file
    blob.download_to_filename(destination_file_name)

    # Load the parquet file into a DataFrame
    df = pd.read_parquet(destination_file_name)
    print(f"File {source_blob_name} downloaded to {destination_file_name}.")

    return df


# Example usage:
# df = download_from_gcs('your-bucket-name', 'remote/path/to/file.parquet', 'local/path/to/file.parquet')


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


def read_parquet(gcs_path: str):
    """Read a Parquet file from a GCS path."""
    # Parse the GCS path
    parsed_url = urlparse(gcs_path)
    bucket_name = parsed_url.netloc
    blob_name = parsed_url.path.lstrip("/")

    # Initialize a GCS client and get the blob
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the blob to an in-memory file
    file_stream = io.BytesIO()
    blob.download_to_file(file_stream)
    file_stream.seek(0)

    # Read the Parquet file
    table = pq.read_table(file_stream)

    # Convert the PyArrow Table to a pandas DataFrame
    dataframe = table.to_pandas()

    return dataframe


def download_to_filename_from_gcs(gcs_path: str, destination_file_name: str) -> None:
    """Read a file from GCS and save it locally.

    Args:
        gcs_path (str): The GCS path to the file.
        destination_file_name (str): The local file path to save the file.
    """
    storage_client = storage.Client()
    bucket_name, blob_name = _parse_gcs_path(gcs_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(destination_file_name)
    logger.info(f"Downloaded {gcs_path} to {destination_file_name}")
