"""Date partitioned migration pattern"""

from typing import Dict, List
import re
from datetime import datetime, timedelta
import sys
import os

# Add the parent directory to the path so we can import base_pattern
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from patterns.base_pattern import BaseMigrationPattern
from utils.logging_utils import log_file_operation


class DatePartitionedPattern(BaseMigrationPattern):
    """Migration pattern for date-partitioned data"""

    def discover_files(self) -> List[Dict]:
        """
        Discover files within specified date range

        Returns:
            List of file info dictionaries with partition date metadata
        """
        source_bucket = self.get_source_bucket()
        source_path = self.get_source_path()
        file_types = self.config.get("file_types", ["*"])
        date_format = self.config.get("date_format", "%Y%m%d")
        partition_pattern = self.config.get("partition_pattern", r"(\d{8})")

        # Date range configuration
        start_date = self._parse_date_config(self.config.get("start_date"))
        end_date = self._parse_date_config(self.config.get("end_date"))
        days_back = self.config.get("days_back")

        self.logger.info(
            f"Discovering date-partitioned files in gs://{source_bucket}/{source_path}"
        )
        self.logger.info(f"Date format: {date_format}")
        self.logger.info(f"Partition pattern: {partition_pattern}")

        # Calculate date range if not explicitly provided
        if not start_date and days_back:
            end_date = end_date or datetime.now()
            start_date = end_date - timedelta(days=days_back)

        if start_date:
            self.logger.info(f"Start date: {start_date.strftime('%Y-%m-%d')}")
        if end_date:
            self.logger.info(f"End date: {end_date.strftime('%Y-%m-%d')}")

        # Get all files first
        all_files = self.gcs_client.list_files(
            bucket=source_bucket,
            prefix=source_path,
            recursive=True,
            file_types=file_types,
        )

        # Filter files based on date partitioning
        date_filtered_files = []
        partition_regex = re.compile(partition_pattern)

        for file_info in all_files:
            file_path = file_info["name"]

            # Extract date from file path
            partition_date = self._extract_partition_date(
                file_path, partition_regex, date_format
            )

            if partition_date:
                # Check if date is within range
                if self._is_date_in_range(partition_date, start_date, end_date):
                    file_info["partition_date"] = partition_date.strftime("%Y%m%d")
                    file_info["partition_date_obj"] = partition_date
                    date_filtered_files.append(file_info)
                    self.logger.debug(
                        f"Included file: {file_path} (date: {partition_date.strftime('%Y-%m-%d')})"
                    )
                else:
                    self.logger.debug(
                        f"Excluded file: {file_path} (date outside range: {partition_date.strftime('%Y-%m-%d')})"
                    )
            else:
                self.logger.debug(f"Excluded file: {file_path} (no date found)")

        # Sort by partition date
        date_filtered_files.sort(key=lambda x: x["partition_date_obj"])

        self.logger.info(
            f"Discovered {len(date_filtered_files)} date-partitioned files"
        )

        # Log date range summary
        if date_filtered_files:
            earliest_date = date_filtered_files[0]["partition_date_obj"]
            latest_date = date_filtered_files[-1]["partition_date_obj"]
            self.logger.info(
                f"Date range of files: {earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
            )

        return date_filtered_files

    def _parse_date_config(self, date_config) -> datetime:
        """
        Parse date configuration which can be string or datetime

        Args:
            date_config: Date configuration (string, datetime, or None)

        Returns:
            Parsed datetime object or None
        """
        if not date_config:
            return None

        if isinstance(date_config, datetime):
            return date_config

        if isinstance(date_config, str):
            # Try different date formats
            date_formats = ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]

            for fmt in date_formats:
                try:
                    return datetime.strptime(date_config, fmt)
                except ValueError:
                    continue

            self.logger.warning(f"Could not parse date: {date_config}")

        return None

    def _extract_partition_date(
        self, file_path: str, partition_regex: re.Pattern, date_format: str
    ) -> datetime:
        """
        Extract partition date from file path

        Args:
            file_path: File path to extract date from
            partition_regex: Compiled regex pattern for date extraction
            date_format: Date format string

        Returns:
            Extracted datetime object or None
        """
        match = partition_regex.search(file_path)
        if not match:
            return None

        date_string = match.group(1)

        try:
            return datetime.strptime(date_string, date_format)
        except ValueError as e:
            self.logger.debug(
                f"Could not parse date '{date_string}' in {file_path}: {e}"
            )
            return None

    def _is_date_in_range(
        self,
        file_date: datetime,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> bool:
        """
        Check if file date is within specified range

        Args:
            file_date: File's partition date
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            True if date is in range, False otherwise
        """
        if start_date and file_date < start_date:
            return False

        if end_date and file_date > end_date:
            return False

        return True

    def calculate_target_path(
        self, source_file_path: str, partition_date: str = None
    ) -> str:
        """
        Calculate target path for date-partitioned file with optional date reorganization

        Args:
            source_file_path: Source file path
            partition_date: Partition date string (YYYYMMDD)

        Returns:
            Target file path
        """
        source_path = self.get_source_path()
        target_path = self.get_target_path()
        target_date_structure = self.config.get("target_date_structure")

        # Remove source path prefix
        if source_file_path.startswith(source_path):
            relative_path = source_file_path[len(source_path) :].lstrip("/")
        else:
            relative_path = source_file_path

        # Apply date restructuring if configured
        if target_date_structure and partition_date:
            relative_path = self._apply_date_restructuring(
                relative_path, partition_date, target_date_structure
            )

        # Combine with target path
        if target_path:
            return f"{target_path.rstrip('/')}/{relative_path}"
        else:
            return relative_path

    def _apply_date_restructuring(
        self, file_path: str, partition_date: str, target_structure: str
    ) -> str:
        """
        Apply date restructuring to file path

        Args:
            file_path: Original file path
            partition_date: Partition date (YYYYMMDD)
            target_structure: Target date structure pattern

        Returns:
            Restructured file path
        """
        try:
            date_obj = datetime.strptime(partition_date, "%Y%m%d")

            # Replace date placeholders in target structure
            formatted_structure = target_structure.format(
                year=date_obj.strftime("%Y"),
                month=date_obj.strftime("%m"),
                day=date_obj.strftime("%d"),
                date=partition_date,
            )

            # Extract filename from original path
            filename = os.path.basename(file_path)

            # Combine new structure with filename
            return f"{formatted_structure}/{filename}"

        except (ValueError, KeyError) as e:
            self.logger.warning(f"Date restructuring failed for {file_path}: {e}")
            return file_path

    def migrate_batch(self, files: List[Dict]) -> bool:
        """
        Migrate a batch of date-partitioned files

        Args:
            files: List of file info dictionaries to migrate

        Returns:
            True if batch migration successful, False otherwise
        """
        source_bucket = self.get_source_bucket()
        target_bucket = self.get_target_bucket()

        self.logger.debug(f"Migrating batch of {len(files)} date-partitioned files")
        self.logger.debug(f"Source bucket: {source_bucket}")
        self.logger.debug(f"Target bucket: {target_bucket}")

        for file_info in files:
            source_file_path = file_info["name"]
            partition_date = file_info.get("partition_date")

            target_file_path = self.calculate_target_path(
                source_file_path, partition_date
            )

            self.logger.debug(f"Migrating: {source_file_path} -> {target_file_path}")
            if partition_date:
                self.logger.debug(f"  Partition date: {partition_date}")

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

    def dry_run(self) -> Dict:
        """
        Enhanced dry run with date partition analysis

        Returns:
            Dictionary with dry run results including date analysis
        """
        results = super().dry_run()

        if "error" in results:
            return results

        try:
            files_to_migrate = self.discover_files()

            # Analyze files by date
            date_stats = {}
            for file_info in files_to_migrate:
                partition_date = file_info.get("partition_date", "unknown")

                if partition_date not in date_stats:
                    date_stats[partition_date] = {
                        "file_count": 0,
                        "total_size": 0,
                        "sample_files": [],
                    }

                date_stats[partition_date]["file_count"] += 1
                date_stats[partition_date]["total_size"] += file_info.get("size", 0)

                if len(date_stats[partition_date]["sample_files"]) < 3:
                    date_stats[partition_date]["sample_files"].append(file_info["name"])

            results["date_analysis"] = date_stats
            results["date_range"] = {
                "start_date": self.config.get("start_date"),
                "end_date": self.config.get("end_date"),
                "days_back": self.config.get("days_back"),
            }

            self.logger.info("Date partition analysis:")
            for partition_date in sorted(date_stats.keys()):
                stats = date_stats[partition_date]
                size_mb = stats["total_size"] / (1024 * 1024)
                self.logger.info(
                    f"  Date {partition_date}: {stats['file_count']} files, {size_mb:.2f} MB"
                )

        except Exception as e:
            self.logger.error(f"Error during date partition analysis: {e}")

        return results

    def cleanup_source_files(self, files: List[Dict]) -> bool:
        """
        Enhanced cleanup with date-based retention policy

        Args:
            files: List of file info dictionaries to clean up

        Returns:
            True if cleanup successful, False otherwise
        """
        if not self.config.get("cleanup_after_migration", False):
            return True

        source_bucket = self.get_source_bucket()
        retention_days = self.config.get("retention_days")

        self.logger.info(
            f"Starting cleanup of {len(files)} date-partitioned source files..."
        )

        if retention_days:
            self.logger.info(f"Applying retention policy: {retention_days} days")

        failed_deletions = []
        skipped_files = []

        for file_info in files:
            # Apply retention policy based on partition date
            if retention_days and "partition_date" in file_info:
                try:
                    file_date = datetime.strptime(file_info["partition_date"], "%Y%m%d")
                    if datetime.now() - file_date <= timedelta(days=retention_days):
                        self.logger.debug(
                            f"Skipping deletion of {file_info['name']} (within retention period)"
                        )
                        skipped_files.append(file_info["name"])
                        continue
                except ValueError:
                    self.logger.warning(
                        f"Could not parse partition date for {file_info['name']}"
                    )

            # Delete the file
            if not self.gcs_client.delete_file(source_bucket, file_info["name"]):
                failed_deletions.append(file_info["name"])

        if skipped_files:
            self.logger.info(
                f"Skipped deletion of {len(skipped_files)} files (within retention period)"
            )

        if failed_deletions:
            self.logger.error(f"Failed to delete {len(failed_deletions)} source files")
            return False

        deleted_count = len(files) - len(skipped_files)
        self.logger.info(f"Successfully cleaned up {deleted_count} source files")
        return True
