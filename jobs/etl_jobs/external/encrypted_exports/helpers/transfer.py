import os
import io
from typing import List, Dict, Any
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from helpers.utils import (
    FILE_EXTENSION,
    ENCRYPTED_FOLDER,
    DEFAULT_MAX_WORKERS,
    init_s3_client,
    logger,
    load_target_bucket_config,
)


def process_transfer(
    partner_name: str,
    gcs_bucket: str,
    export_date: str,
    table_list: List[str],
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    """
    Transfer encrypted parquet files from GCS to an S3-compatible bucket using parallel processing.

    The function:
      - Parses the target bucket configuration (JSON or dictionary-like string).
      - Initializes an S3 client using the provided configuration.
      - For each table, it lists the encrypted parquet files in the specified GCS bucket,
        ensuring that only file blobs (and not directory markers) are processed.
      - Uploads each file to the target S3 bucket in parallel using ThreadPoolExecutor.
      - Tracks and logs the number of successfully transferred files and any failures.

    Args:
        partner_name (str): Name of the partner.
        gcs_bucket (str): GCS bucket name.
        export_date (str): Export date.
        table_list (List[str]): List of tables to process.
    """
    s3_config = load_target_bucket_config(partner_name)
    s3_client = init_s3_client(s3_config)
    encrypted_base = os.path.join(ENCRYPTED_FOLDER, partner_name, export_date)
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
        logger.info(
            f"Transferring {len(file_blobs)} files to {s3_config['target_s3_name']}"
        )

        # Use ThreadPoolExecutor for parallel transfers
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create a list of futures for all file transfers
            future_to_blob = {
                executor.submit(
                    transfer_single_blob, blob, s3_client, s3_config, s3_prefix_export
                ): blob
                for blob in file_blobs
                if blob.name.endswith(FILE_EXTENSION)
            }

            # Process completed transfers as they finish
            for future in as_completed(future_to_blob):
                blob = future_to_blob[future]
                try:
                    if future.result():
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    logger.error(f"Error processing {blob.name}: {e}")
                    fail_count += 1

        total = success_count + fail_count
        if success_count:
            logger.info(
                f"SUCCESS {table}: {success_count}/{total} files transferred to {s3_config['target_s3_name']}"
            )
        if fail_count:
            logger.error(
                f"FAIL {table}: {fail_count}/{total} files NOT transferred to {s3_config['target_s3_name']}"
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
        logger.error(f"ERROR: {blob.name} is empty. Skipping upload.")
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
        logger.error(f"Failed to upload {blob.name}: {e}")
        return False
