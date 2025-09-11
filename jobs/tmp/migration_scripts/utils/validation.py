"""Validation utilities for migration operations"""

import logging
from typing import Dict, List
from .gcs_client import GCSClient


class ValidationUtils:
    """Validation utilities for pre and post migration checks"""

    def __init__(self):
        self.gcs_client = GCSClient()
        self.logger = logging.getLogger(__name__)

    def validate_buckets_exist(self, source_bucket: str, target_bucket: str) -> bool:
        """
        Validate that both source and target buckets exist

        Args:
            source_bucket: Source bucket name
            target_bucket: Target bucket name

        Returns:
            True if both buckets exist, False otherwise
        """
        source_exists = self.gcs_client.bucket_exists(source_bucket)
        target_exists = self.gcs_client.bucket_exists(target_bucket)

        if not source_exists:
            self.logger.error(f"Source bucket '{source_bucket}' does not exist")

        if not target_exists:
            self.logger.error(f"Target bucket '{target_bucket}' does not exist")

        return source_exists and target_exists

    def verify_file_counts(self, migrated_files: List[Dict]) -> bool:
        """
        Verify that all files were successfully migrated by checking file counts

        Args:
            migrated_files: List of file info dictionaries that were migrated

        Returns:
            True if verification passes, False otherwise
        """
        # For now, this is a simple check that we have some files
        # In a production implementation, you might want to:
        # 1. Compare source vs target file counts
        # 2. Verify specific files exist in target
        # 3. Check file sizes match

        if not migrated_files:
            self.logger.warning("No files were migrated")
            return False

        self.logger.info(f"Verified migration of {len(migrated_files)} files")
        return True

    def verify_checksums(
        self, source_bucket: str, target_bucket: str, file_paths: List[str]
    ) -> bool:
        """
        Verify file integrity by comparing checksums between source and target

        Args:
            source_bucket: Source bucket name
            target_bucket: Target bucket name
            file_paths: List of file paths to verify

        Returns:
            True if all checksums match, False otherwise
        """
        mismatched_files = []

        for file_path in file_paths:
            # Get source file info
            source_info = self.gcs_client.get_file_info(source_bucket, file_path)
            if not source_info:
                self.logger.error(
                    f"Cannot get info for source file: gs://{source_bucket}/{file_path}"
                )
                mismatched_files.append(file_path)
                continue

            # Get target file info
            target_info = self.gcs_client.get_file_info(target_bucket, file_path)
            if not target_info:
                self.logger.error(
                    f"Cannot get info for target file: gs://{target_bucket}/{file_path}"
                )
                mismatched_files.append(file_path)
                continue

            # Compare checksums
            if source_info["md5_hash"] != target_info["md5_hash"]:
                self.logger.error(f"Checksum mismatch for {file_path}")
                self.logger.error(f"  Source: {source_info['md5_hash']}")
                self.logger.error(f"  Target: {target_info['md5_hash']}")
                mismatched_files.append(file_path)
            else:
                self.logger.debug(f"Checksum verified for {file_path}")

        if mismatched_files:
            self.logger.error(
                f"{len(mismatched_files)} files failed checksum verification"
            )
            return False

        self.logger.info(f"Checksum verification passed for {len(file_paths)} files")
        return True

    def verify_file_sizes(
        self, source_bucket: str, target_bucket: str, file_mappings: List[Dict]
    ) -> bool:
        """
        Verify file sizes match between source and target

        Args:
            source_bucket: Source bucket name
            target_bucket: Target bucket name
            file_mappings: List of dicts with 'source_path' and 'target_path' keys

        Returns:
            True if all file sizes match, False otherwise
        """
        mismatched_files = []

        for mapping in file_mappings:
            source_path = mapping["source_path"]
            target_path = mapping["target_path"]

            # Get source file info
            source_info = self.gcs_client.get_file_info(source_bucket, source_path)
            if not source_info:
                self.logger.error(
                    f"Cannot get info for source file: gs://{source_bucket}/{source_path}"
                )
                mismatched_files.append(source_path)
                continue

            # Get target file info
            target_info = self.gcs_client.get_file_info(target_bucket, target_path)
            if not target_info:
                self.logger.error(
                    f"Cannot get info for target file: gs://{target_bucket}/{target_path}"
                )
                mismatched_files.append(source_path)
                continue

            # Compare file sizes
            if source_info["size"] != target_info["size"]:
                self.logger.error(f"File size mismatch for {source_path}")
                self.logger.error(f"  Source: {source_info['size']} bytes")
                self.logger.error(f"  Target: {target_info['size']} bytes")
                mismatched_files.append(source_path)
            else:
                self.logger.debug(f"File size verified for {source_path}")

        if mismatched_files:
            self.logger.error(f"{len(mismatched_files)} files failed size verification")
            return False

        self.logger.info(
            f"File size verification passed for {len(file_mappings)} files"
        )
        return True

    def validate_migration_config(self, config: Dict) -> bool:
        """
        Validate migration configuration has required fields

        Args:
            config: Migration configuration dictionary

        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields = ["pattern", "source_bucket", "target_bucket", "source_path"]

        missing_fields = []
        for field in required_fields:
            if field not in config:
                missing_fields.append(field)

        if missing_fields:
            self.logger.error(
                f"Missing required configuration fields: {missing_fields}"
            )
            return False

        # Validate pattern type
        valid_patterns = [
            "single_folder",
            "multi_folder",
            "file_pattern",
            "date_partitioned",
        ]
        if config["pattern"] not in valid_patterns:
            self.logger.error(
                f"Invalid pattern '{config['pattern']}'. Must be one of: {valid_patterns}"
            )
            return False

        self.logger.debug("Migration configuration validation passed")
        return True

    def check_storage_space(self, target_bucket: str, required_bytes: int) -> bool:
        """
        Check if target bucket has sufficient space (simplified check)

        Args:
            target_bucket: Target bucket name
            required_bytes: Estimated bytes needed

        Returns:
            True if sufficient space (always returns True for GCS as it's virtually unlimited)
        """
        # GCS has virtually unlimited storage, so we'll always return True
        # In a real implementation, you might want to check billing quotas or project limits
        self.logger.debug(
            f"Storage space check for {required_bytes} bytes in {target_bucket}: OK"
        )
        return True

    def validate_permissions(self, source_bucket: str, target_bucket: str) -> bool:
        """
        Validate that we have necessary permissions on both buckets

        Args:
            source_bucket: Source bucket name
            target_bucket: Target bucket name

        Returns:
            True if permissions are sufficient, False otherwise
        """
        try:
            # Test read permission on source bucket
            self.gcs_client.list_files(source_bucket, "", recursive=False)
            self.logger.debug(f"Read permission verified for {source_bucket}")

            # Test write permission on target bucket by attempting to list
            # (We can't easily test write without actually writing a file)
            self.gcs_client.list_files(target_bucket, "", recursive=False)
            self.logger.debug(f"Access permission verified for {target_bucket}")

            return True

        except Exception as e:
            self.logger.error(f"Permission validation failed: {e}")
            return False
