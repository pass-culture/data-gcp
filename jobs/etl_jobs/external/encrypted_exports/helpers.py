import os
import io
import json
import duckdb
import boto3
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from typing import List, Dict, Any, Tuple, Optional
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage, secretmanager
from botocore.client import Config
import concurrent.futures
import concurrent.futures

FILE_EXTENSION = ".parquet"
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
PREFIX_S3_SECRET = "dbt_export_s3_config"


def prepare_directories(
    partner_name: str,
    table: str,
    tmp_download: str,
    tmp_encrypted_folder: str,
    export_date: str,
) -> Tuple[str, str, str]:
    """
    Create and return necessary local directory paths.

    Args:
        partner_name (str): Partner name.
        table (str): Table name.
        tmp_download (str): Temporary download directory.
        tmp_encrypted_folder (str): Temporary encrypted folder directory.
        export_date (str): Export date.

    Returns:
        Tuple[str, str, str]: A tuple containing:
            - local_base: Local directory for downloads.
            - encrypted_base: Local directory for encrypted files.
            - gcs_encrypted_folder_path: GCS folder path where encrypted files are uploaded.
    """
    local_base = os.path.join(tmp_download, partner_name, table)
    encrypted_base = os.path.join(tmp_encrypted_folder, partner_name, table)
    gcs_encrypted_folder_path = os.path.join(
        tmp_encrypted_folder, partner_name, export_date, table
    )
    for d in [tmp_encrypted_folder, local_base, encrypted_base]:
        Path(d).mkdir(parents=True, exist_ok=True)
    return local_base, encrypted_base, gcs_encrypted_folder_path


def encrypt_and_upload_file(
    bucket: storage.bucket.Bucket,
    duckdb_conn,
    parquet_file: str,
    local_base: str,
    encrypted_base: str,
    gcs_encrypted_folder_path: str,
) -> None:
    """
    Download a parquet file from GCS, encrypt it using DuckDB, upload the encrypted file back to GCS,
    and then remove the local temporary files.

    Args:
        bucket (storage.bucket.Bucket): GCS bucket object.
        duckdb_conn: DuckDB connection object.
        parquet_file (str): The path of the parquet file in GCS.
        local_base (str): Local directory for downloads.
        encrypted_base (str): Local directory for encrypted files.
        gcs_encrypted_folder_path (str): GCS folder path where encrypted files are uploaded.
    """
    filename = os.path.basename(parquet_file)
    local_file_path = os.path.join(local_base, filename)
    encrypted_file_path = os.path.join(encrypted_base, filename)

    # Download the original parquet file from GCS
    blob = bucket.blob(parquet_file)
    blob.download_to_filename(local_file_path)

    # Encrypt the file using DuckDB
    duckdb_conn.execute(
        f"""COPY (SELECT * FROM read_parquet('{local_file_path}'))
            TO '{encrypted_file_path}'
            (FORMAT 'parquet', ENCRYPTION_CONFIG {{footer_key: 'key256'}});"""
    )

    # Upload the encrypted file to GCS
    encrypted_blob = bucket.blob(os.path.join(gcs_encrypted_folder_path, filename))
    encrypted_blob.upload_from_filename(encrypted_file_path)

    # Cleanup temporary files
    os.remove(local_file_path)
    os.remove(encrypted_file_path)


def load_target_bucket_config(partner_name: str) -> Dict[str, Any]:
    secret_id = f"{PREFIX_S3_SECRET}_{partner_name}"

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_NAME}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        access_secret_data = response.payload.data.decode("UTF-8")
        return json.loads(access_secret_data)
    except DefaultCredentialsError:
        return {}


def init_s3_client(s3_config: Dict[str, Any]) -> boto3.client:
    """
    Initialize and return an S3 client using the provided configuration.

    Args:
        s3_config (Dict[str, Any]): The configuration dictionary for S3.

    Returns:
        boto3.client: An S3 client instance.
    """
    # Store configuration state with a session
    _ = boto3.session.Session()
    return boto3.client(
        "s3",
        aws_access_key_id=s3_config["target_access_key"],
        aws_secret_access_key=s3_config["target_secret_key"],
        endpoint_url=s3_config["target_endpoint_url"],
        region_name=s3_config["target_s3_region"],
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def transfer_single_blob(
    blob: storage.blob.Blob, s3_client, s3_config: Dict[str, Any], s3_prefix_export: str
) -> bool:
    """
    Transfer a single GCS blob to S3.

    Args:
        blob (storage.blob.Blob): GCS blob to transfer.
        s3_client: Initialized S3 client.
        s3_config (Dict[str, Any]): S3 configuration dictionary.
        s3_prefix_export (str): The S3 key prefix for the upload.

    Returns:
        bool: True if the transfer was successful, False otherwise.
    """
    if not blob.name.endswith(FILE_EXTENSION):
        return True  # Skip non-parquet files silently

    # Download blob as a byte stream
    gcs_stream = io.BytesIO(blob.download_as_bytes())
    if gcs_stream.getbuffer().nbytes == 0:
        print(f"ERROR: {blob.name} is empty. Skipping upload.")
        return False

    s3_object_name = f"{s3_prefix_export}/{os.path.basename(blob.name)}"
    try:
        s3_client.put_object(
            Bucket=s3_config["target_s3_name"],
            Key=s3_object_name,
            Body=gcs_stream.read(),
        )
        return True
    except Exception as e:
        print(f"Failed to upload {blob.name}: {e}")
        return False


def _process_single_table(
def _process_single_table(
    partner_name: str,
    gcs_bucket: str,
    export_date: str,
    table: str,
    table: str,
    encryption_key: str,
    client: storage.Client,
    tmp_download: str,
    tmp_encrypted_folder: str,
) -> Optional[Tuple[str, int]]:
    """
    Process encryption for a single table.
    client: storage.Client,
    tmp_download: str,
    tmp_encrypted_folder: str,
) -> Optional[Tuple[str, int]]:
    """
    Process encryption for a single table.

    Args:
        partner_name (str): Name of the partner.
        gcs_bucket (str): GCS bucket name.
        export_date (str): Export date.
        table (str): Table name to process.
        table (str): Table name to process.
        encryption_key (str): A 32-character encryption key.
        client (storage.Client): GCS client.
        tmp_download (str): Temporary download directory.
        tmp_encrypted_folder (str): Temporary encrypted folder directory.

    Returns:
        Optional[Tuple[str, int]]: Table name and number of files processed, or None if failed.
    """
    try:
        bucket = client.get_bucket(gcs_bucket)

        # Prepare local directories and get the GCS encrypted folder path
        local_base, encrypted_base, gcs_encrypted_folder_path = prepare_directories(
            partner_name, table, tmp_download, tmp_encrypted_folder, export_date
        )

        # Define the GCS folder path for original parquet files
        gcs_folder_path = f"{partner_name}/{export_date}/{table}"
        print(f"gcs folder: {gcs_folder_path}")

        # List parquet files from GCS
        blobs = bucket.list_blobs(prefix=gcs_folder_path)
        parquet_file_list = [
            blob.name for blob in blobs if blob.name.endswith(FILE_EXTENSION)
        ]
        assert len(parquet_file_list) != 0, f"{gcs_bucket}/{gcs_folder_path} is empty"

        # Process each parquet file
        def process_single_file(parquet_file):
            duckdb_conn = duckdb.connect(config={"threads": 1})
            duckdb_conn.execute(
                f"PRAGMA add_parquet_key('key256', '{encryption_key}');"
            )
            print(f"parquet file: {parquet_file}")
            encrypt_and_upload_file(
                bucket,
                duckdb_conn,
                parquet_file,
                local_base,
                encrypted_base,
                gcs_encrypted_folder_path,
            )
            duckdb_conn.close()
            print(f"encrypted file: {parquet_file}")
            return True

        # Use ThreadPoolExecutor to process files in parallel
        max_workers = min(
            32, len(parquet_file_list)
        )  # Limit max workers to avoid overwhelming the system
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and wait for completion
            futures = [
                executor.submit(process_single_file, parquet_file)
                for parquet_file in parquet_file_list
            ]
            # Wait for all futures to complete
            concurrent.futures.wait(futures)
            # Get the count of successfully processed files
            file_count = sum(1 for future in futures if future.result())

        print(
            f"Table {table} successfully encrypted in {file_count} files -> gs://{gcs_bucket}/{gcs_encrypted_folder_path}"
            f"Table {table} successfully encrypted in {file_count} files -> gs://{gcs_bucket}/{gcs_encrypted_folder_path}"
        )

        return table, file_count
    except Exception as e:
        print(f"Error processing table {table}: {e}")
        return None


def process_encryption(
    partner_name: str,
    gcs_bucket: str,
    export_date: str,
    table_list: List[str],
    encryption_key: str,
) -> None:
    """
    Encrypt parquet files for each table in parallel.

    For each table, this function:
      - Connects to DuckDB and registers the provided encryption key.
      - Creates temporary directories for downloads and for the encrypted files.
      - Downloads the parquet files from the GCS bucket.
      - Encrypts the downloaded file using DuckDB.
      - Uploads the encrypted file back to GCS.
      - Removes temporary files.

    Args:
        partner_name (str): Name of the partner.
        gcs_bucket (str): GCS bucket name.
        export_date (str): Export date.
        table_list (List[str]): List of tables to process.
        encryption_key (str): A 32-character encryption key.
    """
    tmp_encrypted_folder = "tmp_encrypted_parquet"
    tmp_download = "tmp_downloads"

    for table in table_list:
        table, file_count = _process_single_table(
            partner_name,
            gcs_bucket,
            export_date,
            table,
            encryption_key,
            client=storage.Client(),
            tmp_download=tmp_download,
            tmp_encrypted_folder=tmp_encrypted_folder,
        )
        print(f"Completed processing for table {table} with {file_count} files")


def process_transfer(
    partner_name: str,
    gcs_bucket: str,
    export_date: str,
    table_list: List[str],
) -> None:
    """
    Transfer encrypted parquet files from GCS to an S3-compatible bucket.

    The function:
      - Parses the target bucket configuration (JSON or dictionary-like string).
      - Initializes an S3 client using the provided configuration.
      - For each table, it lists the encrypted parquet files in the specified GCS bucket,
        ensuring that only file blobs (and not directory markers) are processed.
      - Uploads each file to the target S3 bucket.
      - Tracks and prints the number of successfully transferred files and any failures.

    Args:
        partner_name (str): Name of the partner.
        gcs_bucket (str): GCS bucket name.
        export_date (str): Export date.
        table_list (List[str]): List of tables to process.
    """
    s3_config = load_target_bucket_config(partner_name)

    s3_client = init_s3_client(s3_config)
    tmp_encrypted_folder = "tmp_encrypted_parquet"
    encrypted_base = os.path.join(tmp_encrypted_folder, partner_name, export_date)
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket)

    for table in table_list:
        # Define the GCS folder path for encrypted files
        gcs_folder_path = os.path.join(encrypted_base, table)
        print(f"gcs folder: {gcs_folder_path}")

        # List all blobs under the encrypted folder and filter out directory markers
        blobs = list(bucket.list_blobs(prefix=gcs_folder_path))
        file_blobs = [blob for blob in blobs if not blob.name.endswith("/")]

        # Ensure there is at least one parquet file to transfer
        parquet_file_list = [
            blob.name for blob in file_blobs if blob.name.endswith(FILE_EXTENSION)
        ]
        assert len(parquet_file_list) != 0, f"{gcs_bucket}/{gcs_folder_path} is empty"

        prefix_target_bucket = s3_config.get("target_s3_prefix", "passculture_export")
        s3_prefix_export = f"{prefix_target_bucket}/{export_date}/{table}"

        success_count = 0
        fail_count = 0

        for blob in file_blobs:
            if blob.name.endswith(FILE_EXTENSION):
                if transfer_single_blob(blob, s3_client, s3_config, s3_prefix_export):
                    success_count += 1
                else:
                    fail_count += 1

        total = success_count + fail_count
        if success_count:
            print(
                f"SUCCESS {table}: {success_count}/{total} files transferred to {s3_config['target_s3_name']}"
            )
        if fail_count:
            print(
                f"FAIL {table}: {fail_count}/{total} files NOT transferred to {s3_config['target_s3_name']}"
            )
