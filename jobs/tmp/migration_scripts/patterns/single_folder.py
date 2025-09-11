"""Single folder migration pattern"""

from typing import Dict, List
import sys
import os

# Add the parent directory to the path so we can import base_pattern
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from patterns.base_pattern import BaseMigrationPattern
from utils.logging_utils import log_file_operation


class SingleFolderPattern(BaseMigrationPattern):
    """Migration pattern for entire folder contents"""

    def discover_files(self) -> List[Dict]:
        """
        Discover all files in source folder

        Returns:
            List of file info dictionaries
        """
        source_bucket = self.get_source_bucket()
        source_path = self.get_source_path()
        include_subfolders = self.config.get("include_subfolders", False)
        file_types = self.config.get("file_types", ["*"])

        self.logger.info(f"Discovering files in gs://{source_bucket}/{source_path}")
        self.logger.info(f"Include subfolders: {include_subfolders}")
        self.logger.info(f"File types: {file_types}")

        files = self.gcs_client.list_files(
            bucket=source_bucket,
            prefix=source_path,
            recursive=include_subfolders,
            file_types=file_types,
        )

        self.logger.info(f"Discovered {len(files)} files")
        return files

    def migrate_batch(self, files: List[Dict]) -> bool:
        """
        Migrate a batch of files

        Args:
            files: List of file info dictionaries to migrate

        Returns:
            True if batch migration successful, False otherwise
        """
        source_bucket = self.get_source_bucket()
        target_bucket = self.get_target_bucket()

        self.logger.debug(f"Migrating batch of {len(files)} files")
        self.logger.debug(f"Source bucket: {source_bucket}")
        self.logger.debug(f"Target bucket: {target_bucket}")

        for file_info in files:
            source_file_path = file_info["name"]
            target_file_path = self.calculate_target_path(source_file_path)

            self.logger.debug(f"Migrating: {source_file_path} -> {target_file_path}")

            # Copy the file
            success = self.gcs_client.copy_file(
                source_bucket=source_bucket,
                source_path=source_file_path,
                target_bucket=target_bucket,
                target_path=target_file_path,
            )

            # Log the operation
            log_file_operation(
                self.logger,
                "COPY",
                f"gs://{source_bucket}/{source_file_path}",
                f"gs://{target_bucket}/{target_file_path}",
                success,
            )

            if not success:
                self.logger.error(f"Failed to copy {source_file_path}")
                return False

            # Track progress
            self.progress_tracker.log_file_processed(file_info, success)

            # Store target path in file info for validation
            file_info["target_path"] = target_file_path

        return True
