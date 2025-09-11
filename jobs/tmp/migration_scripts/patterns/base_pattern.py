"""Base migration pattern class"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import logging
import sys
import os

# Add the parent directory to the path so we can import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.gcs_client import GCSClient
from utils.validation import ValidationUtils
from utils.logging_utils import MigrationProgressTracker


class BaseMigrationPattern(ABC):
    """Base class for all migration patterns"""

    def __init__(self, config: Dict, env: str):
        """
        Initialize base migration pattern

        Args:
            config: Migration configuration dictionary
            env: Environment (dev, stg, prod)
        """
        self.config = config
        self.env = env
        self.gcs_client = GCSClient()
        self.validator = ValidationUtils()
        self.logger = logging.getLogger(f"migration.{self.__class__.__name__}")
        self.progress_tracker = MigrationProgressTracker(self.logger)

    @abstractmethod
    def discover_files(self) -> List[Dict]:
        """
        Discover files to migrate based on pattern

        Returns:
            List of file info dictionaries
        """
        pass

    @abstractmethod
    def migrate_batch(self, files: List[Dict]) -> bool:
        """
        Migrate a batch of files

        Args:
            files: List of file info dictionaries to migrate

        Returns:
            True if batch migration successful, False otherwise
        """
        pass

    def pre_migration_validation(self) -> bool:
        """
        Validate conditions before starting migration

        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info("Starting pre-migration validation...")

        # Validate configuration
        if not self.validator.validate_migration_config(self.config):
            return False

        # Check if buckets exist
        source_bucket = self.config["source_bucket"].format(env=self.env)
        target_bucket = self.config["target_bucket"].format(env=self.env)

        if not self.validator.validate_buckets_exist(source_bucket, target_bucket):
            return False

        # Check permissions
        if not self.validator.validate_permissions(source_bucket, target_bucket):
            return False

        self.logger.info("Pre-migration validation passed")
        return True

    def post_migration_validation(self, migrated_files: List[Dict]) -> bool:
        """
        Validate migration results after completion

        Args:
            migrated_files: List of file info dictionaries that were migrated

        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info("Starting post-migration validation...")

        # Basic file count validation
        if not self.validator.verify_file_counts(migrated_files):
            return False

        validation_config = self.config.get("validation", {})

        # Optional checksum verification
        if validation_config.get("verify_checksums", False):
            source_bucket = self.config["source_bucket"].format(env=self.env)
            target_bucket = self.config["target_bucket"].format(env=self.env)

            # Get file paths for verification
            file_paths = []
            for file_info in migrated_files:
                if "target_path" in file_info:
                    file_paths.append(file_info["target_path"])
                else:
                    # Use source path if target path not specified
                    file_paths.append(file_info["name"])

            if not self.validator.verify_checksums(
                source_bucket, target_bucket, file_paths
            ):
                return False

        # Optional file size verification
        if validation_config.get("verify_file_sizes", False):
            source_bucket = self.config["source_bucket"].format(env=self.env)
            target_bucket = self.config["target_bucket"].format(env=self.env)

            # Create file mappings for size verification
            file_mappings = []
            for file_info in migrated_files:
                mapping = {
                    "source_path": file_info["name"],
                    "target_path": file_info.get("target_path", file_info["name"]),
                }
                file_mappings.append(mapping)

            if not self.validator.verify_file_sizes(
                source_bucket, target_bucket, file_mappings
            ):
                return False

        self.logger.info("Post-migration validation passed")
        return True

    def cleanup_source_files(self, files: List[Dict]) -> bool:
        """
        Clean up source files after successful migration

        Args:
            files: List of file info dictionaries to clean up

        Returns:
            True if cleanup successful, False otherwise
        """
        if not self.config.get("cleanup_after_migration", False):
            return True

        source_bucket = self.config["source_bucket"].format(env=self.env)
        retention_days = self.config.get("retention_days")

        self.logger.info(f"Starting cleanup of {len(files)} source files...")

        failed_deletions = []
        for file_info in files:
            # Apply retention policy if specified
            if retention_days and "partition_date" in file_info:
                from datetime import datetime, timedelta

                file_date = datetime.strptime(file_info["partition_date"], "%Y%m%d")
                if datetime.now() - file_date <= timedelta(days=retention_days):
                    self.logger.debug(
                        f"Skipping deletion of {file_info['name']} (within retention period)"
                    )
                    continue

            # Delete the file
            if not self.gcs_client.delete_file(source_bucket, file_info["name"]):
                failed_deletions.append(file_info["name"])

        if failed_deletions:
            self.logger.error(f"Failed to delete {len(failed_deletions)} source files")
            return False

        self.logger.info(f"Successfully cleaned up {len(files)} source files")
        return True

    def execute(self) -> bool:
        """
        Main execution method for migration pattern

        Returns:
            True if migration successful, False otherwise
        """
        self.logger.info(f"Starting migration for pattern: {self.__class__.__name__}")
        self.logger.info(f"Configuration: {self.config}")

        # Pre-migration validation
        if not self.pre_migration_validation():
            self.logger.error("Pre-migration validation failed")
            return False

        # Discover files to migrate
        try:
            files_to_migrate = self.discover_files()
            self.logger.info(f"Discovered {len(files_to_migrate)} files to migrate")

            if not files_to_migrate:
                self.logger.warning("No files found to migrate")
                return True  # Not an error if no files to migrate

        except Exception as e:
            self.logger.error(f"Error discovering files: {e}")
            return False

        # Migrate files in batches
        batch_size = self.config.get("batch_size", 100)
        migrated_files = []
        total_batches = (len(files_to_migrate) + batch_size - 1) // batch_size

        self.logger.info(
            f"Migrating files in {total_batches} batches of {batch_size} files each"
        )

        for i in range(0, len(files_to_migrate), batch_size):
            batch_num = i // batch_size + 1
            batch = files_to_migrate[i : i + batch_size]

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} files)"
            )

            try:
                if self.migrate_batch(batch):
                    migrated_files.extend(batch)
                    self.logger.info(f"Batch {batch_num} completed successfully")
                else:
                    self.logger.error(f"Batch {batch_num} failed")
                    return False

            except Exception as e:
                self.logger.error(f"Error in batch {batch_num}: {e}")
                return False

            # Log progress
            self.progress_tracker.log_progress(len(files_to_migrate))

        # Post-migration validation
        if not self.post_migration_validation(migrated_files):
            self.logger.error("Post-migration validation failed")
            return False

        # Optional cleanup
        if self.config.get("cleanup_after_migration", False):
            if not self.cleanup_source_files(migrated_files):
                self.logger.warning(
                    "Source file cleanup failed, but migration was successful"
                )

        # Log final statistics
        self.progress_tracker.log_final_stats()
        self.logger.info(
            f"Migration completed successfully: {len(migrated_files)} files migrated"
        )

        return True

    def dry_run(self) -> Dict:
        """
        Perform a dry run to analyze what would be migrated

        Returns:
            Dictionary with dry run results
        """
        self.logger.info(f"Starting dry run for pattern: {self.__class__.__name__}")

        try:
            files_to_migrate = self.discover_files()

            # Calculate total size
            total_size = sum(file_info.get("size", 0) for file_info in files_to_migrate)

            # Categorize files by type
            file_types = {}
            for file_info in files_to_migrate:
                file_name = file_info["name"]
                extension = (
                    file_name.split(".")[-1].lower()
                    if "." in file_name
                    else "no_extension"
                )
                file_types[extension] = file_types.get(extension, 0) + 1

            source_bucket = self.config["source_bucket"].format(env=self.env)
            target_bucket = self.config["target_bucket"].format(env=self.env)

            results = {
                "source_bucket": source_bucket,
                "target_bucket": target_bucket,
                "total_files": len(files_to_migrate),
                "total_size_bytes": total_size,
                "total_size_mb": total_size / (1024 * 1024),
                "file_types": file_types,
                "sample_files": files_to_migrate[:5],  # First 5 files as sample
            }

            self.logger.info(f"Dry run results:")
            self.logger.info(f"  Source bucket: {source_bucket}")
            self.logger.info(f"  Target bucket: {target_bucket}")
            self.logger.info(f"  Files to migrate: {len(files_to_migrate)}")
            self.logger.info(f"  Total size: {results['total_size_mb']:.2f} MB")
            self.logger.info(f"  File types: {file_types}")

            if files_to_migrate:
                self.logger.info("Sample files:")
                for file_info in files_to_migrate[:5]:
                    size_mb = file_info.get("size", 0) / (1024 * 1024)
                    self.logger.info(f"    {file_info['name']} ({size_mb:.2f} MB)")

            return results

        except Exception as e:
            self.logger.error(f"Error during dry run: {e}")
            return {"error": str(e)}

    def get_source_bucket(self) -> str:
        """Get formatted source bucket name"""
        return self.config["source_bucket"].format(env=self.env)

    def get_target_bucket(self) -> str:
        """Get formatted target bucket name"""
        return self.config["target_bucket"].format(env=self.env)

    def get_source_path(self) -> str:
        """Get source path from configuration"""
        return self.config["source_path"]

    def get_target_path(self) -> str:
        """Get target path from configuration"""
        return self.config.get("target_path", "")

    def calculate_target_path(self, source_file_path: str) -> str:
        """
        Calculate target path for a source file

        Args:
            source_file_path: Source file path

        Returns:
            Target file path
        """
        source_path = self.get_source_path()
        target_path = self.get_target_path()

        # Remove source path prefix and add target path prefix
        if source_file_path.startswith(source_path):
            relative_path = source_file_path[len(source_path) :].lstrip("/")
        else:
            relative_path = source_file_path

        if target_path:
            return f"{target_path.rstrip('/')}/{relative_path}"
        else:
            return relative_path
