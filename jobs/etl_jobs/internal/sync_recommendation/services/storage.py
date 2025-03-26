import logging
import os
from pathlib import Path
from typing import List

from google.api_core import retry
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage

logger = logging.getLogger(__name__)


class StorageService:
    def __init__(self, project_id: str):
        self.client = storage.Client(project=project_id)

    def check_files_exists(self, bucket_path: str) -> bool:
        """
        Check if files exist in the given GCS path.
        The path can contain wildcards (*) and will check for any matching files.

        Args:
            bucket_path: GCS path (e.g., 'gs://bucket/path/*.parquet')

        Returns:
            bool: True if files exist, False otherwise
        """
        try:
            # Extract bucket name and prefix from the path
            path_parts = bucket_path.replace("gs://", "").split("/")
            bucket_name = path_parts[0]
            prefix = "/".join(path_parts[1:])

            # Get the bucket
            bucket = self.client.bucket(bucket_name)

            # List blobs matching the prefix
            blobs = list(bucket.list_blobs(prefix=prefix))

            if not blobs:
                logger.warning(f"No files found at {bucket_path}")
                return False

            logger.info(f"Found {len(blobs)} files at {bucket_path}")
            return True

        except Exception as e:
            logger.error(f"Error checking files existence at {bucket_path}: {str(e)}")
            return False

    def download_files(
        self, bucket_path: str, prefix: str, destination_dir: str
    ) -> List[str]:
        """
        Args:
            bucket_path: GCS bucket path (gs://bucket-name/path)
            prefix: file prefix
            destination_dir: local directory to download files to

        Returns:
            List of local file paths to downloaded Parquet files
        """
        bucket_folder_path = "/".join(bucket_path.replace("gs://", "").split("/")[1:])
        bucket_name = bucket_path.replace("gs://", "").split("/")[0]
        bucket = self.client.bucket(bucket_name)

        logger.info(bucket_folder_path + prefix)

        downloaded_files = []
        for blob in bucket.list_blobs(prefix=Path(bucket_folder_path) / prefix):
            logger.info(blob.name)
            local_path = Path(destination_dir) / blob.name.split("/")[-1]
            blob.download_to_filename(str(local_path))
            downloaded_files.append(str(local_path))
            logger.info(f"Downloaded {blob.name} to {local_path}")

        return downloaded_files

    def upload_files(self, source_paths: List[str], destination_dir: str) -> List[str]:
        """
        Upload multiple files to GCS.

        Args:
            source_paths: List of local file paths to upload
            destination_dir: GCS directory path (e.g., 'gs://bucket/path/')

        Returns:
            List of GCS paths where files were uploaded

        Raises:
            ValueError: If source_paths is empty or destination_dir is invalid
            FileNotFoundError: If any source file doesn't exist
            GoogleAPIError: If GCS upload fails
        """
        if not source_paths:
            raise ValueError("source_paths cannot be empty")

        if not destination_dir.startswith("gs://"):
            raise ValueError("destination_dir must start with 'gs://'")

        # Extract bucket name and base path from the destination directory
        path_parts = destination_dir.replace("gs://", "").split("/")
        bucket_name = path_parts[0]
        if not bucket_name:
            raise ValueError("Invalid bucket name in destination_dir")

        base_path = "/".join(path_parts[1:]).rstrip("/")

        bucket = self.client.bucket(bucket_name)
        uploaded_paths = []

        for source_path in source_paths:
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Source file not found: {source_path}")

            try:
                # Get the filename from the source path
                filename = os.path.basename(source_path)
                # Construct the full GCS path
                blob_path = f"{base_path}/{filename}" if base_path else filename

                # Upload the file with retry logic
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(
                    source_path,
                    timeout=300,  # 5 minutes timeout
                    retry=retry.Retry(
                        initial=1.0,
                        maximum=60.0,
                        multiplier=2,
                        deadline=300,
                        predicate=retry.if_exception_type(
                            GoogleAPIError,
                        ),
                    ),
                )

                gcs_path = f"gs://{bucket_name}/{blob_path}"
                uploaded_paths.append(gcs_path)
                logger.info(f"Successfully uploaded {source_path} to {gcs_path}")

            except GoogleAPIError as e:
                logger.error(f"Failed to upload {source_path}: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error uploading {source_path}: {str(e)}")
                raise

        if not uploaded_paths:
            raise RuntimeError("No files were successfully uploaded")

        return uploaded_paths

    def cleanup_files(self, file_paths: List[str]) -> None:
        """Clean up local files"""
        for file_path in file_paths:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Removed temporary file: {file_path}")
