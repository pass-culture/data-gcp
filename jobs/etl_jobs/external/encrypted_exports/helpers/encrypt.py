import os
import duckdb
from pathlib import Path
from typing import List, Tuple, Optional
from google.cloud import storage
import concurrent.futures
import time

from helpers.utils import (
    FILE_EXTENSION,
    logger,
    get_optimal_worker_count,
    get_optimal_batch_size,
)


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
    encryption_key: str,
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
        logger.info(f"gcs folder: {gcs_folder_path}")

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
        logger.error(f"Error processing table {table}: {e}")
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
            logger.error(f"Failed to process table {table}")
            continue
        table, file_count = result
