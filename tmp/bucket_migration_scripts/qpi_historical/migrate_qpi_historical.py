#!/usr/bin/env python3
"""
QPI Historical Data Migration Script

This script migrates all QPI_historical data from the old bucket structure to the new
dedicated seed data bucket.

Migration path:
- From: gs://data-bucket-{env}/QPI_historical/
- To: gs://de-bigquery-data-import-{env}/historical/

Usage:
    python migrate_qpi_historical.py --env prod --dry-run
    python migrate_qpi_historical.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple


class QPIHistoricalDataMigrator:
    """Handles migration of QPI_historical data from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-import-{env}"
        self.old_path = f"gs://{self.old_bucket}/QPI_historical"
        self.new_path = f"gs://{self.new_bucket}/QPI_historical"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_qpi_historical_{env}_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def check_bucket_exists(self, bucket_name: str) -> bool:
        """Check if a GCS bucket exists."""
        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", f"gs://{bucket_name}"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.returncode == 0
        except subprocess.SubprocessError:
            return False

    def list_qpi_historical_contents(self) -> List[str]:
        """List all contents in the /QPI_historical folder."""
        self.logger.info(f"Scanning contents of {self.old_path}")

        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", f"{self.old_path}/"],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                if (
                    "BucketNotFoundException" in result.stderr
                    or "not found" in result.stderr
                ):
                    self.logger.info("QPI_historical folder does not exist or is empty")
                    return []
                else:
                    self.logger.error(
                        f"Error listing QPI_historical contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning QPI_historical folder: {e}")
            return []

    def get_folder_size(self, path: str) -> str:
        """Get total size of folder using gcloud storage du."""
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "storage",
                    "du",
                    "--summarize",
                    "--readable-sizes",
                    f"{path}/",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0 and result.stdout:
                # Output format: "123.4 MiB  gs://bucket/path"
                size_info = result.stdout.strip().split()[0:2]
                return " ".join(size_info) if len(size_info) >= 2 else "unknown"
            else:
                return "unknown"

        except subprocess.SubprocessError:
            return "unknown"

    def migrate_qpi_historical_data(self) -> Tuple[bool, int]:
        """
        Migrate all QPI_historical data using gcloud storage rsync.

        Returns:
            Tuple of (success, files_count)
        """
        self.logger.info(f"Starting QPI_historical data migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_qpi_historical_contents()
        if not files:
            self.logger.info("No QPI_historical data to migrate")
            return True, 0

        self.logger.info(f"Found {len(files)} files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        if self.dry_run:
            self.logger.info("DRY RUN - QPI_historical data that would be migrated:")
            # Show sample of files (first 10 and last 10 if more than 20 files)
            if len(files) <= 20:
                for file_path in files:
                    relative_path = file_path.replace(f"{self.old_path}/", "")
                    self.logger.info(f"  {file_path} → {self.new_path}/{relative_path}")
            else:
                self.logger.info("Sample of files to be migrated (first 10):")
                for file_path in files[:10]:
                    relative_path = file_path.replace(f"{self.old_path}/", "")
                    self.logger.info(f"  {file_path} → {self.new_path}/{relative_path}")
                self.logger.info(f"  ... and {len(files) - 20} more files ...")
                self.logger.info("Last 10 files:")
                for file_path in files[-10:]:
                    relative_path = file_path.replace(f"{self.old_path}/", "")
                    self.logger.info(f"  {file_path} → {self.new_path}/{relative_path}")

            return True, len(files)

        # Perform actual migration using gcloud storage rsync
        try:
            cmd = [
                "gcloud",
                "storage",
                "rsync",
                "--recursive",
                f"{self.old_path}",
                f"{self.new_path}",
            ]

            self.logger.info(f"Executing: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode == 0:
                self.logger.info("Successfully migrated QPI_historical data")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files)
            else:
                self.logger.error("QPI_historical data migration failed")
                self.logger.error(f"gcloud storage error: {result.stderr}")
                return False, 0

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during migration: {e}")
            return False, 0

    def verify_migration(self) -> bool:
        """Verify that data was successfully migrated."""
        if self.dry_run:
            return True

        try:
            # Check if data exists in new location
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", self.new_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                new_files = [
                    line
                    for line in result.stdout.strip().split("\n")
                    if line and not line.endswith("/")
                ]
                self.logger.info(
                    f"Verification: {len(new_files)} files found in new location"
                )
                return len(new_files) > 0
            else:
                self.logger.warning(
                    "Could not verify migration - new path not accessible"
                )
                return False

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during verification: {e}")
            return False

    def run_migration(self) -> bool:
        """Run the complete migration process."""
        self.logger.info("=" * 60)
        self.logger.info(f"Starting QPI_historical data migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info("=" * 60)

        # Check if old bucket exists
        if not self.check_bucket_exists(self.old_bucket):
            self.logger.error(f"Source bucket gs://{self.old_bucket} does not exist!")
            return False

        # Check if new bucket exists
        if not self.check_bucket_exists(self.new_bucket):
            self.logger.error(f"Target bucket gs://{self.new_bucket} does not exist!")
            return False

        # Perform migration
        success, files_migrated = self.migrate_qpi_historical_data()

        if not success:
            self.logger.error("Migration failed")
            return False

        # Verify migration if not dry run
        if not self.dry_run and files_migrated > 0:
            if not self.verify_migration():
                self.logger.warning("Migration verification failed")

        # Summary
        self.logger.info("=" * 60)
        self.logger.info("MIGRATION SUMMARY")
        self.logger.info(f"Files migrated: {files_migrated}")
        self.logger.info(f"Status: {'SUCCESS' if success else 'FAILED'}")
        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        self.logger.info("=" * 60)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Migrate QPI_historical data to new bucket structure"
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "stg", "prod"],
        help="Environment to migrate (dev/stg/prod)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Perform dry run without actual migration (default)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute actual migration (overrides --dry-run)",
    )

    args = parser.parse_args()

    # If --execute is specified, disable dry-run
    dry_run = not args.execute

    if not dry_run:
        response = input(
            f"Are you sure you want to migrate QPI_historical data for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = QPIHistoricalDataMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
