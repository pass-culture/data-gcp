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
import multiprocessing
import psutil
import threading
from loguru import logger
import time
import sys

# Configure loguru
logger.remove()  # Remove default handler
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)

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


def get_optimal_worker_count(file_count: int) -> int:
    """Calculate optimal number of workers based on file count."""
    cpu_count = multiprocessing.cpu_count()
    return min(cpu_count, file_count)  # Use all available CPUs


def get_optimal_batch_size(file_count: int, worker_count: int) -> int:
    """Calculate optimal batch size based on file count and worker count."""
    # Make batch size a multiple of worker count
    base_batch = min(20, file_count)  # Base batch size
    # Round up to nearest multiple of worker count
    return ((base_batch + worker_count - 1) // worker_count) * worker_count


def process_single_file(
    parquet_file: str,
    encrypted_base: str,
    local_base: str,
    gcs_encrypted_folder_path: str,
    gcs_bucket: str,
    encryption_key: str,
) -> bool:
    """Process a single parquet file with proper error handling."""
    start_time = time.time()
    try:
        # Create GCS client inside the worker process
        client = storage.Client()
        bucket = client.get_bucket(gcs_bucket)

        # Optimize DuckDB for performance
        duckdb_conn = duckdb.connect(
            config={
                "threads": 1,  # Single thread per worker
                "memory_limit": "1GB",  # Limit memory per worker
            }
        )
        duckdb_conn.execute(f"PRAGMA add_parquet_key('key256', '{encryption_key}');")

        logger.debug(f"Starting encryption of {parquet_file}")
        encrypt_and_upload_file(
            bucket,
            duckdb_conn,
            parquet_file,
            local_base,
            encrypted_base,
            gcs_encrypted_folder_path,
        )
        duckdb_conn.close()
        duration = time.time() - start_time
        logger.debug(f"Completed {parquet_file} in {duration:.2f}s")
        return True
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"Failed to process {parquet_file} after {duration:.2f}s: {str(e)}"
        )
        return False
    finally:
        if "duckdb_conn" in locals():
            duckdb_conn.close()


def download_single_file(
    parquet_file: str, bucket: storage.bucket.Bucket, local_base: str
) -> bool:
    """Download a single file from GCS with retry logic."""
    max_retries = 3
    retry_delay = 1  # seconds

    for attempt in range(max_retries):
        try:
            blob = bucket.blob(parquet_file)
            local_path = os.path.join(local_base, os.path.basename(parquet_file))
            # Use larger chunks for faster downloads
            blob.download_to_filename(
                local_path, chunk_size=4 * 1024 * 1024
            )  # 4MB chunks
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    f"Retry {attempt + 1}/{max_retries} for {parquet_file}: {str(e)}"
                )
                time.sleep(retry_delay * (attempt + 1))
            else:
                logger.error(
                    f"Failed to download {parquet_file} after {max_retries} attempts: {str(e)}"
                )
                return False


def process_batch(
    batch: List[str],
    executor: concurrent.futures.ProcessPoolExecutor,
    encrypted_base: str,
    local_base: str,
    gcs_encrypted_folder_path: str,
    gcs_bucket: str,
    encryption_key: str,
) -> int:
    """Process a batch of files and return the number of successful operations."""
    start_time = time.time()
    futures = [
        executor.submit(
            process_single_file,
            parquet_file,
            encrypted_base,
            local_base,
            gcs_encrypted_folder_path,
            gcs_bucket,
            encryption_key,
        )
        for parquet_file in batch
    ]
    concurrent.futures.wait(futures)
    success_count = sum(1 for future in futures if future.result())
    duration = time.time() - start_time
    logger.info(
        f"Batch completed: {success_count}/{len(batch)} files in {duration:.2f}s"
    )
    return success_count


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

        # Calculate optimal parameters based on file count
        file_count = len(parquet_file_list)
        max_workers = get_optimal_worker_count(file_count)
        batch_size = get_optimal_batch_size(file_count, max_workers)

        logger.info(
            f"Starting processing of {file_count} files with {max_workers} workers in batches of {batch_size}"
        )

        file_count = 0
        total_start_time = time.time()

        try:
            # Process files in batches
            for i in range(0, len(parquet_file_list), batch_size):
                batch_start_time = time.time()
                current_batch = parquet_file_list[i : i + batch_size]

                # Process current batch
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    batch_success_count = process_batch(
                        current_batch,
                        executor,
                        encrypted_base,
                        local_base,
                        gcs_encrypted_folder_path,
                        gcs_bucket,
                        encryption_key,
                    )
                    file_count += batch_success_count
                    batch_duration = time.time() - batch_start_time
                    files_per_second = len(current_batch) / batch_duration
                    logger.info(
                        f"Batch {i//batch_size + 1} completed: "
                        f"{batch_success_count}/{len(current_batch)} files in {batch_duration:.2f}s "
                        f"({files_per_second:.2f} files/s)"
                    )

        except Exception as e:
            total_duration = time.time() - total_start_time
            logger.error(
                f"Error during batch processing after {total_duration:.2f}s: {str(e)}"
            )
            raise

        total_duration = time.time() - total_start_time
        total_files_per_second = file_count / total_duration
        logger.success(
            f"Table {table} completed: {file_count} files processed in {total_duration:.2f}s "
            f"({total_files_per_second:.2f} files/s) -> gs://{gcs_bucket}/{gcs_encrypted_folder_path}"
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
        result = _process_single_table(
            partner_name,
            gcs_bucket,
            export_date,
            table,
            encryption_key,
            client=storage.Client(),
            tmp_download=tmp_download,
            tmp_encrypted_folder=tmp_encrypted_folder,
        )
        if result is None:
            print(f"Failed to process table {table}")
            continue
        table, file_count = result


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
