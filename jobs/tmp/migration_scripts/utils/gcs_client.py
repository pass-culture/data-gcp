"""GCS Client wrapper for bucket operations"""

import logging
from typing import Dict, List, Optional
from google.cloud import storage
from google.api_core import exceptions
import fnmatch


class GCSClient:
    """Google Cloud Storage client wrapper for migration operations"""

    def __init__(self):
        self.client = storage.Client()
        self.logger = logging.getLogger(__name__)

    def list_files(
        self,
        bucket: str,
        prefix: str = "",
        recursive: bool = True,
        file_types: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        List files in a GCS bucket with optional filtering

        Args:
            bucket: Bucket name
            prefix: Path prefix to filter files
            recursive: Whether to include subdirectories
            file_types: List of file patterns to match (e.g., ['*.csv', '*.parquet'])

        Returns:
            List of file info dictionaries with name, size, modified time
        """
        try:
            bucket_obj = self.client.bucket(bucket)

            # Configure delimiter for non-recursive listing
            delimiter = None if recursive else "/"

            blobs = bucket_obj.list_blobs(prefix=prefix, delimiter=delimiter)

            files = []
            for blob in blobs:
                # Skip directories (they end with /)
                if blob.name.endswith("/"):
                    continue

                # Apply file type filtering if specified
                if file_types and file_types != ["*"]:
                    file_name = blob.name.split("/")[-1]
                    matches_pattern = any(
                        fnmatch.fnmatch(file_name, pattern) for pattern in file_types
                    )
                    if not matches_pattern:
                        continue

                file_info = {
                    "name": blob.name,
                    "size": blob.size,
                    "modified": blob.updated,
                    "md5_hash": blob.md5_hash,
                    "etag": blob.etag,
                }
                files.append(file_info)

            self.logger.info(f"Found {len(files)} files in gs://{bucket}/{prefix}")
            return files

        except exceptions.NotFound:
            self.logger.error(f"Bucket {bucket} not found")
            return []
        except Exception as e:
            self.logger.error(f"Error listing files in gs://{bucket}/{prefix}: {e}")
            return []

    def list_folders(self, bucket: str, prefix: str = "") -> List[str]:
        """
        List folders (prefixes) in a GCS bucket

        Args:
            bucket: Bucket name
            prefix: Path prefix to filter folders

        Returns:
            List of folder paths
        """
        try:
            bucket_obj = self.client.bucket(bucket)
            blobs = bucket_obj.list_blobs(prefix=prefix, delimiter="/")

            folders = []
            # Get prefixes (folders) from the iterator
            for page in blobs.pages:
                folders.extend(page.prefixes)

            return folders

        except exceptions.NotFound:
            self.logger.error(f"Bucket {bucket} not found")
            return []
        except Exception as e:
            self.logger.error(f"Error listing folders in gs://{bucket}/{prefix}: {e}")
            return []

    def copy_file(
        self, source_bucket: str, source_path: str, target_bucket: str, target_path: str
    ) -> bool:
        """
        Copy a file from source bucket to target bucket

        Args:
            source_bucket: Source bucket name
            source_path: Source file path
            target_bucket: Target bucket name
            target_path: Target file path

        Returns:
            True if successful, False otherwise
        """
        try:
            source_bucket_obj = self.client.bucket(source_bucket)
            source_blob = source_bucket_obj.blob(source_path)

            target_bucket_obj = self.client.bucket(target_bucket)

            # Copy the blob
            copy_blob = source_bucket_obj.copy_blob(
                source_blob, target_bucket_obj, target_path
            )

            self.logger.debug(
                f"Copied gs://{source_bucket}/{source_path} -> gs://{target_bucket}/{target_path}"
            )
            return True

        except exceptions.NotFound as e:
            self.logger.error(f"File not found during copy: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error copying file: {e}")
            return False

    def delete_file(self, bucket: str, file_path: str) -> bool:
        """
        Delete a file from GCS bucket

        Args:
            bucket: Bucket name
            file_path: File path to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            bucket_obj = self.client.bucket(bucket)
            blob = bucket_obj.blob(file_path)
            blob.delete()

            self.logger.debug(f"Deleted gs://{bucket}/{file_path}")
            return True

        except exceptions.NotFound:
            self.logger.warning(
                f"File gs://{bucket}/{file_path} not found for deletion"
            )
            return True  # Consider it successful if file doesn't exist
        except Exception as e:
            self.logger.error(f"Error deleting file gs://{bucket}/{file_path}: {e}")
            return False

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists

        Args:
            bucket_name: Bucket name to check

        Returns:
            True if bucket exists, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            bucket.reload()  # This will raise an exception if bucket doesn't exist
            return True
        except exceptions.NotFound:
            return False
        except Exception as e:
            self.logger.error(f"Error checking bucket {bucket_name}: {e}")
            return False

    def get_file_info(self, bucket: str, file_path: str) -> Optional[Dict]:
        """
        Get detailed information about a specific file

        Args:
            bucket: Bucket name
            file_path: File path

        Returns:
            Dictionary with file information or None if not found
        """
        try:
            bucket_obj = self.client.bucket(bucket)
            blob = bucket_obj.blob(file_path)
            blob.reload()  # Fetch metadata

            return {
                "name": blob.name,
                "size": blob.size,
                "modified": blob.updated,
                "created": blob.time_created,
                "md5_hash": blob.md5_hash,
                "etag": blob.etag,
                "content_type": blob.content_type,
            }

        except exceptions.NotFound:
            return None
        except Exception as e:
            self.logger.error(
                f"Error getting file info for gs://{bucket}/{file_path}: {e}"
            )
            return None

    def create_bucket(self, bucket_name: str, location: str = "US") -> bool:
        """
        Create a new GCS bucket

        Args:
            bucket_name: Name for the new bucket
            location: GCS location (default: US)

        Returns:
            True if successful, False otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            bucket = self.client.create_bucket(bucket, location=location)

            self.logger.info(f"Created bucket {bucket_name} in {location}")
            return True

        except exceptions.Conflict:
            self.logger.info(f"Bucket {bucket_name} already exists")
            return True  # Consider it successful if bucket already exists
        except Exception as e:
            self.logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False
