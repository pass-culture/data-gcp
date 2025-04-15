import logging
from pathlib import Path
from typing import List, Tuple

from google.api_core import retry
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import storage

logger = logging.getLogger(__name__)


class StorageService:
    def __init__(self, project_id: str):
        self.client = storage.Client(project=project_id)

    def extract_bucket_name_and_folder_path(self, bucket_path: str) -> Tuple[str, str]:
        """
        Extract bucket name and folder path from a GCS path.

        Args:
            bucket_path: GCS path (e.g., 'gs://bucket/path/*.parquet')

        Returns:
            Tuple containing (bucket_name, folder_path)

        Raises:
            ValueError: If bucket_path is not a valid GCS path
        """
        if not bucket_path.startswith("gs://"):
            raise ValueError("bucket_path must start with 'gs://'")

        path_parts = bucket_path.replace("gs://", "").split("/")
        if not path_parts[0]:
            raise ValueError("Invalid bucket name in bucket_path")

        bucket_name = path_parts[0]
        folder_path = "/".join(path_parts[1:])
        return bucket_name, folder_path

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
            bucket_name, prefix = self.extract_bucket_name_and_folder_path(bucket_path)
            bucket = self.client.bucket(bucket_name)

            # List blobs matching the prefix
            blobs = list(bucket.list_blobs(prefix=prefix))

            if not blobs:
                logger.warning(f"No files found matching pattern: {bucket_path}")
                return False

            logger.info(f"Found {len(blobs)} files matching pattern: {bucket_path}")
            return True

        except (ValueError, NotFound) as e:
            logger.warning(f"Error checking files existence at {bucket_path}: {str(e)}")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error checking files existence at {bucket_path}: {str(e)}"
            )
            return False

    def download_files(
        self, bucket_path: str, prefix: str, destination_dir: str
    ) -> List[str]:
        """
        Download files from GCS to local directory.

        Args:
            bucket_path: GCS bucket path (gs://bucket-name/path)
            prefix: file prefix
            destination_dir: local directory to download files to

        Returns:
            List of local file paths to downloaded Parquet files

        Raises:
            ValueError: If bucket_path is invalid
            FileNotFoundError: If destination directory doesn't exist
        """
        bucket_name, bucket_folder_path = self.extract_bucket_name_and_folder_path(
            bucket_path
        )
        bucket = self.client.bucket(bucket_name)

        destination_path = Path(destination_dir)
        if not destination_path.exists():
            raise FileNotFoundError(
                f"Destination directory does not exist: {destination_dir}"
            )

        logger.info(f"Downloading files from {bucket_path} with prefix {prefix}")

        downloaded_files = []
        for blob in bucket.list_blobs(prefix=Path(bucket_folder_path) / prefix):
            local_path = destination_path / Path(blob.name).name
            blob.download_to_filename(str(local_path))
            downloaded_files.append(str(local_path))
            logger.info(f"Downloaded {blob.name} to {local_path}")

        if not downloaded_files:
            logger.warning(
                f"No files found to download from {bucket_path} with prefix {prefix}"
            )

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

        bucket_name, base_path = self.extract_bucket_name_and_folder_path(
            destination_dir
        )
        bucket = self.client.bucket(bucket_name)
        uploaded_paths = []

        for source_path in source_paths:
            source_path_obj = Path(source_path)
            if not source_path_obj.exists():
                raise FileNotFoundError(f"Source file not found: {source_path}")

            try:
                # Construct the full GCS path using Path
                blob_path = (
                    str(Path(base_path) / source_path_obj.name)
                    if base_path
                    else source_path_obj.name
                )

                # Upload the file with retry logic
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(
                    str(source_path_obj),
                    timeout=300,  # 5 minutes timeout
                    retry=retry.Retry(
                        initial=1.0,
                        maximum=60.0,
                        multiplier=2,
                        deadline=300,
                        predicate=retry.if_exception_type(GoogleAPIError),
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
            path = Path(file_path)
            if path.exists():
                path.unlink()
                logger.info(f"Removed temporary file: {file_path}")
