import concurrent.futures
import time
from contextlib import contextmanager
from pathlib import Path
from typing import List, Tuple

import duckdb
from fsspec import filesystem
from google.cloud import storage
from helpers.utils import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    ENCRYPTED_FOLDER,
    logger,
)


@contextmanager
def get_duckdb_connection(encryption_key: str):
    """
    Context manager for DuckDB connection with encryption key setup.

    Args:
        encryption_key (str): The encryption key to use for parquet encryption.

    Yields:
        duckdb.DuckDBPyConnection: Configured DuckDB connection.
    """
    conn = duckdb.connect(
        config={
            "threads": 1,  # Single thread per worker
        }
    )
    try:
        conn.execute(f"PRAGMA add_parquet_key('key256', '{encryption_key}');")
        conn.register_filesystem(filesystem("gcs"))

        yield conn
    finally:
        conn.close()


def get_parquet_files(bucket_name: str, prefix: str) -> List[str]:
    """
    List all parquet files in a GCS bucket with the given prefix.

    Args:
        bucket_name (str): GCS bucket name
        prefix (str): Prefix to filter files

    Returns:
        List[str]: List of parquet file paths
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix + "/")

    # Filter for parquet files only
    parquet_files = [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".parquet")
    ]
    return parquet_files


def process_batch(
    batch_files: List[str], batch_num: int, output_gcs_path: str, encryption_key: str
) -> Tuple[int, bool, float]:
    """
    Process a single batch of files.

    Args:
        batch_files (List[str]): List of parquet files to process
        batch_num (int): Batch number
        output_gcs_path (str): Output GCS path
        encryption_key (str): Encryption key

    Returns:
        Tuple[int, bool, float]: (batch number, success status, processing time)
    """
    try:
        batch_start_time = time.time()
        with get_duckdb_connection(encryption_key) as conn:
            # Create the query with explicit file list
            files_list = "', '".join(batch_files)
            query = f"""
            COPY (
                    SELECT
                        *
                    FROM read_parquet(['{files_list}'])
                )
            TO '{output_gcs_path}'
            (FORMAT 'parquet', ENCRYPTION_CONFIG {{footer_key: 'key256'}}, COMPRESSION 'zstd');
            """

            conn.execute(query)
            batch_duration = time.time() - batch_start_time
            files_per_second = (
                len(batch_files) / batch_duration if batch_duration > 0 else 0
            )

            logger.debug(
                f"Encrypted batch {batch_num} ({len(batch_files)} files) to {output_gcs_path} "
                f"in {batch_duration:.2f}s ({files_per_second:.2f} files/s)"
            )
            return batch_num, True, batch_duration
    except Exception as e:
        logger.error(f"Error processing batch {batch_num}: {e}")
        return batch_num, False, 0.0


def process_encryption(
    partner_name: str,
    gcs_bucket: str,
    export_date: str,
    table_list: List[str],
    encryption_key: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    """
    Encrypt parquet files directly from GCS to GCS using DuckDB.
    Processes files in batches of 50 in parallel and combines them into numbered encrypted output files.

    Args:
        partner_name (str): Name of the partner.
        gcs_bucket (str): GCS bucket name.
        export_date (str): Export date.
        table_list (List[str]): List of tables to process.
        encryption_key (str): A 32-character encryption key.
    """

    for table in table_list:
        try:
            start_time = time.time()

            # Define input and output paths
            input_folder = Path(partner_name) / export_date / table / ""
            output_folder = Path(ENCRYPTED_FOLDER) / partner_name / export_date / table

            # Get list of all parquet files
            input_prefix = str(input_folder)
            parquet_files = get_parquet_files(gcs_bucket, input_prefix)

            if not parquet_files:
                logger.warning(f"No parquet files found in {input_prefix}")
                continue

            # Process files in batches
            total_files = len(parquet_files)
            logger.info(
                f"Total files to process: {total_files}, batch size: {batch_size}, will process {len(range(0, total_files, batch_size))} batches"
            )

            # Create batch processing tasks
            batch_tasks = []
            for batch_idx, i in enumerate(range(0, total_files, batch_size)):
                batch_files = parquet_files[i : i + batch_size]
                batch_num = batch_idx + 1
                output_gcs_path = f"gs://{str(Path(gcs_bucket) / output_folder)}/{batch_num:012d}.parquet"

                batch_tasks.append((batch_files, batch_num, output_gcs_path))

            # Process batches in parallel using ProcessPoolExecutor
            successful_batches = 0
            total_processing_time = 0.0
            processed_files = 0

            with concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers
            ) as executor:
                # Submit all batch processing tasks
                future_to_batch = {
                    executor.submit(
                        process_batch,
                        batch_files,
                        batch_num,
                        output_gcs_path,
                        encryption_key,
                    ): batch_num
                    for batch_files, batch_num, output_gcs_path in batch_tasks
                }

                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_num = future_to_batch[future]
                    try:
                        _, success, batch_duration = future.result()
                        if success:
                            successful_batches += 1
                            total_processing_time += batch_duration
                            processed_files += len(
                                batch_tasks[batch_num - 1][0]
                            )  # Get number of files in this batch

                            # Log running statistics
                            current_duration = time.time() - start_time
                            current_rate = (
                                processed_files / current_duration
                                if current_duration > 0
                                else 0
                            )
                            logger.info(
                                f"Progress: {processed_files}/{total_files} files processed "
                                f"({(processed_files/total_files*100):.1f}%) "
                                f"at {current_rate:.2f} files/s"
                            )
                    except Exception as e:
                        logger.error(f"Batch {batch_num} generated an exception: {e}")

            duration = time.time() - start_time
            overall_rate = total_files / duration if duration > 0 else 0
            avg_batch_rate = (
                processed_files / total_processing_time
                if total_processing_time > 0
                else 0
            )

            logger.success(
                f"Table {table} completed: processed {total_files} files in {len(batch_tasks)} batches "
                f"({successful_batches} successful) in {duration:.2f}s\n"
                f"Overall processing rate: {overall_rate:.2f} files/s\n"
                f"Average batch processing rate: {avg_batch_rate:.2f} files/s"
            )

        except Exception as e:
            logger.error(f"Error processing table {table}: {e}")
            continue
