#!/usr/bin/env python3
"""
Historical Data Migration Script

This script migrates historical data from the old bucket structure to the new
dedicated archive buckets, based on the bucket migration implemented in commit d216232ee.

Migration paths:
- historization/tracking/ → de-bigquery-data-archive-{env}/historization/tracking/
- historization/int_firebase/ → de-bigquery-data-archive-{env}/historization/int_firebase/
- historization/api_reco/ → ds-data-archive-{env}/historization/api_reco/
- historization/applicative/ → de-bigquery-data-archive-{env}/historization/applicative/

Usage:
    python migrate_historical_data.py --env prod --days 30 --dry-run
    python migrate_historical_data.py --env prod --days 30 --execute
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from typing import List, Tuple
from dataclasses import dataclass


@dataclass
class MigrationPath:
    """Defines a migration path from old to new bucket structure."""

    old_folder: str
    new_bucket: str
    new_folder: str
    description: str


class HistoricalDataMigrator:
    """Handles migration of historical data from old to new bucket structure."""

    def __init__(self, env: str, days: int, dry_run: bool = True):
        self.env = env
        self.days = days
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(
                    f'migration_{env}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
                ),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

        # Define migration paths based on the new bucket structure
        self.migration_paths = [
            # MigrationPath(
            #     old_folder="historization/tracking",
            #     new_bucket=f"de-bigquery-data-archive-{env}",
            #     new_folder="historization/tracking",
            #     description="Firebase tracking events",
            # ),
            # MigrationPath(
            #     old_folder="historization/int_firebase",
            #     new_bucket=f"de-bigquery-data-archive-{env}",
            #     new_folder="historization/int_firebase",
            #     description="Firebase intermediate events",
            # ),
            # MigrationPath(
            #     old_folder="historization/api_reco",
            #     new_bucket=f"ds-data-archive-{env}",
            #     new_folder="historization/api_reco",
            #     description="API recommendation data",
            # ),
            MigrationPath(
                old_folder="historization/applicative",
                new_bucket=f"de-bigquery-data-archive-{env}",
                new_folder="historization/applicative",
                description="Applicative database snapshots",
            ),
        ]

    def get_date_range(self) -> List[str]:
        """Generate list of date strings for the last N days."""
        end_date = datetime.now()
        dates = []

        for i in range(self.days):
            date = end_date - timedelta(days=i)
            dates.append(date.strftime("%Y%m%d"))

        return dates

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

    def get_cache_filename(self, migration_path: MigrationPath) -> str:
        """Generate cache filename for the file list."""
        safe_folder = migration_path.old_folder.replace("/", "_")
        return f"file_list_cache_{self.env}_{safe_folder}_{self.days}days.txt"

    def save_file_list_to_cache(
        self, migration_path: MigrationPath, files: List[str]
    ) -> None:
        """Save file list to cache file."""
        cache_file = self.get_cache_filename(migration_path)
        try:
            with open(cache_file, "w") as f:
                for file_path in files:
                    f.write(f"{file_path}\n")
            self.logger.info(f"Saved {len(files)} file paths to cache: {cache_file}")
        except IOError as e:
            self.logger.warning(f"Failed to save cache file {cache_file}: {e}")

    def load_file_list_from_cache(self, migration_path: MigrationPath) -> List[str]:
        """Load file list from cache file if it exists."""
        cache_file = self.get_cache_filename(migration_path)
        if not os.path.exists(cache_file):
            return []

        try:
            with open(cache_file, "r") as f:
                files = [line.strip() for line in f if line.strip()]
            self.logger.info(f"Loaded {len(files)} file paths from cache: {cache_file}")
            return files
        except IOError as e:
            self.logger.warning(f"Failed to load cache file {cache_file}: {e}")
            return []

    def list_old_data(self, migration_path: MigrationPath) -> List[str]:
        """List existing data in the old bucket structure for given date range."""
        # Try to load from cache first
        cached_files = self.load_file_list_from_cache(migration_path)
        if cached_files:
            self.logger.info(f"Using cached file list ({len(cached_files)} files)")
            return cached_files

        # Cache miss - scan the bucket
        self.logger.info("Cache miss - scanning bucket for files...")
        old_path = f"gs://{self.old_bucket}/{migration_path.old_folder}"

        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", old_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                self.logger.warning(f"No data found in {old_path}")
                return []

            # Filter files based on date range
            date_strings = self.get_date_range()
            files = []

            self.logger.info(
                f"Filtering files by date range ({len(date_strings)} dates)"
            )
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    # Check if file contains any of our target dates
                    if any(date in line for date in date_strings):
                        files.append(line)

            # Save to cache for future runs
            if files:
                self.save_file_list_to_cache(migration_path, files)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error listing files from {old_path}: {e}")
            return []

    def migrate_path(self, migration_path: MigrationPath) -> Tuple[bool, int, int]:
        """
        Migrate data for a specific path.

        Returns:
            Tuple of (success, files_migrated, files_failed)
        """
        self.logger.info(f"Starting migration: {migration_path.description}")
        self.logger.info(f"  From: gs://{self.old_bucket}/{migration_path.old_folder}")
        self.logger.info(
            f"  To: gs://{migration_path.new_bucket}/{migration_path.new_folder}"
        )

        # Check if target bucket exists
        # if not self.check_bucket_exists(migration_path.new_bucket):
        #     self.logger.error(
        #         f"Target bucket gs://{migration_path.new_bucket} does not exist!"
        #     )
        #     return False, 0, 0

        # Get list of files to migrate
        files_to_migrate = self.list_old_data(migration_path)

        if not files_to_migrate:
            self.logger.info(f"No files to migrate for {migration_path.description}")
            return True, 0, 0

        self.logger.info(f"Found {len(files_to_migrate)} files to migrate")

        if self.dry_run:
            self.logger.info("DRY RUN - Files that would be migrated:")
            for file_path in files_to_migrate:
                # Extract relative path from old structure
                relative_path = file_path.replace(
                    f"gs://{self.old_bucket}/{migration_path.old_folder}", ""
                )
                new_path = f"gs://{migration_path.new_bucket}/{migration_path.new_folder}/{relative_path}"
                self.logger.info(f"  {file_path} → {new_path}")
            return True, len(files_to_migrate), 0

        # Choose migration strategy based on folder structure
        self.logger.info(
            f"Starting optimized migration for {migration_path.old_folder}..."
        )

        try:
            old_base_path = f"gs://{self.old_bucket}/{migration_path.old_folder}"
            new_base_path = (
                f"gs://{migration_path.new_bucket}/{migration_path.new_folder}"
            )
            date_strings = self.get_date_range()

            if "applicative" in migration_path.old_folder:
                return self._migrate_applicative_structure(
                    old_base_path, new_base_path, date_strings
                )
            elif "api_reco" in migration_path.old_folder:
                return self._migrate_api_reco_structure(old_base_path, new_base_path)
            else:
                # Fallback to generic migration
                return self._migrate_generic_structure(old_base_path, new_base_path)

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during migration: {e}")
            return False, 0, 0

    def _migrate_applicative_structure(
        self, old_base_path: str, new_base_path: str, date_strings: List[str]
    ) -> Tuple[bool, int, int]:
        """Migrate applicative data: subfolder/YYYYMMDD/files structure."""
        subfolders = [
            "booking",
            "collective_booking",
            "collective_offer",
            "deposit",
            "offer",
            "offer_removed",
            "offerer",
            "stock",
            "venue",
        ]

        migrated_count = 0
        failed_count = 0
        total_operations = len(subfolders) * len(date_strings)
        operation_idx = 0

        self.logger.info(
            f"Applicative migration: {len(subfolders)} subfolders × {len(date_strings)} dates = {total_operations} operations"
        )

        for subfolder in subfolders:
            self.logger.info(f"Processing subfolder: {subfolder}")

            for date_str in date_strings:
                operation_idx += 1

                source_path = f"{old_base_path}/{subfolder}/{date_str}"
                dest_path = f"{new_base_path}/{subfolder}/{date_str}"

                # Progress logging every 5% or for each subfolder start
                if (
                    operation_idx % max(1, total_operations // 20) == 0
                    or operation_idx == 1
                ):
                    progress_pct = (operation_idx / total_operations) * 100
                    self.logger.info(
                        f"Progress: [{operation_idx}/{total_operations}] ({progress_pct:.1f}%) - {subfolder}/{date_str}"
                    )

                success, synced = self._rsync_single_folder(source_path, dest_path)
                if success and synced > 0:
                    migrated_count += synced
                elif not success:
                    failed_count += 1

        return self._log_migration_result(migrated_count, failed_count, "Applicative")

    def _migrate_api_reco_structure(
        self, old_base_path: str, new_base_path: str
    ) -> Tuple[bool, int, int]:
        """Migrate api_reco data: rsync the entire folder structure."""
        self.logger.info(f"API Reco migration: Syncing entire folder structure")

        # Simply rsync the entire api_reco folder
        rsync_cmd = [
            "gcloud",
            "storage",
            "rsync",
            "--recursive",
            old_base_path,
            new_base_path,
        ]

        self.logger.info(
            f"Syncing entire api_reco folder: {old_base_path} → {new_base_path}"
        )
        self.logger.debug(
            f"Command: gcloud storage rsync --recursive {old_base_path} {new_base_path}"
        )

        result = subprocess.run(rsync_cmd, capture_output=True, text=True, check=False)

        if result.returncode == 0:
            files_synced = 0
            if result.stdout:
                sync_lines = [
                    line
                    for line in result.stdout.strip().split("\n")
                    if "Copying" in line or "Uploading" in line
                ]
                files_synced = len(sync_lines)

            if files_synced > 0:
                self.logger.info(f"✓ Synced {files_synced} files from api_reco")
            else:
                self.logger.info(f"→ No files to sync (already up to date)")

            return True, files_synced, 0
        else:
            self.logger.error(f"✗ Failed to sync api_reco folder")
            self.logger.error(f"  Error: {result.stderr.strip()}")
            return False, 0, 1

    def _migrate_generic_structure(
        self, old_base_path: str, new_base_path: str
    ) -> Tuple[bool, int, int]:
        """Fallback migration for other structures."""
        self.logger.info("Using generic rsync migration")

        rsync_cmd = [
            "gcloud",
            "storage",
            "rsync",
            "--recursive",
            old_base_path,
            new_base_path,
        ]

        result = subprocess.run(rsync_cmd, capture_output=True, text=True, check=False)

        if result.returncode == 0:
            files_synced = 0
            if result.stdout:
                sync_lines = [
                    line
                    for line in result.stdout.strip().split("\n")
                    if "Copying" in line or "Uploading" in line
                ]
                files_synced = len(sync_lines)
            return True, files_synced, 0
        else:
            self.logger.error(f"Generic migration failed: {result.stderr.strip()}")
            return False, 0, 1

    def _rsync_single_folder(
        self, source_path: str, dest_path: str
    ) -> Tuple[bool, int]:
        """Perform rsync on a single folder."""
        # Quick check if source exists
        check_cmd = ["gcloud", "storage", "ls", source_path]
        check_result = subprocess.run(
            check_cmd, capture_output=True, text=True, check=False
        )

        if check_result.returncode != 0:
            return True, 0  # No data, but not an error

        rsync_cmd = [
            "gcloud",
            "storage",
            "rsync",
            "--recursive",
            source_path,
            dest_path,
        ]

        result = subprocess.run(rsync_cmd, capture_output=True, text=True, check=False)

        if result.returncode == 0:
            files_synced = 0
            if result.stdout:
                sync_lines = [
                    line
                    for line in result.stdout.strip().split("\n")
                    if "Copying" in line or "Uploading" in line
                ]
                files_synced = len(sync_lines)
            return True, files_synced
        else:
            self.logger.error(
                f"  Rsync failed for {source_path}: {result.stderr.strip()}"
            )
            return False, 0

    def _log_migration_result(
        self, migrated_count: int, failed_count: int, migration_type: str
    ) -> Tuple[bool, int, int]:
        """Log and return migration results."""
        self.logger.info(
            f"{migration_type} migration completed: {migrated_count} files synced, {failed_count} operations failed"
        )

        if failed_count == 0:
            return True, migrated_count, 0
        else:
            return False, migrated_count, failed_count

    def verify_migration(self, migration_path: MigrationPath) -> bool:
        """Verify that data was successfully migrated."""
        if self.dry_run:
            return True

        old_files = self.list_old_data(migration_path)
        if not old_files:
            return True

        # Check if files exist in new location
        new_path = f"gs://{migration_path.new_bucket}/{migration_path.new_folder}/"

        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", new_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                self.logger.error(
                    f"Cannot verify migration - new path {new_path} not accessible"
                )
                return False

            new_files = [
                line
                for line in result.stdout.strip().split("\n")
                if line and not line.endswith("/")
            ]

            # Simple count comparison (could be enhanced with checksums)
            if len(new_files) >= len(old_files):
                self.logger.info(
                    f"Verification passed: {len(new_files)} files in new location"
                )
                return True
            else:
                self.logger.warning(
                    f"Verification concern: {len(old_files)} old files, {len(new_files)} new files"
                )
                return False

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during verification: {e}")
            return False

    def run_migration(self) -> bool:
        """Run the complete migration process."""
        self.logger.info("=" * 60)
        self.logger.info(f"Starting historical data migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Days to migrate: {self.days}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info("=" * 60)

        total_success = True
        total_migrated = 0
        total_failed = 0

        for migration_path in self.migration_paths:
            success, migrated, failed = self.migrate_path(migration_path)

            if not success:
                total_success = False

            total_migrated += migrated
            total_failed += failed

            # Verify migration if not dry run
            if not self.dry_run and success:
                if not self.verify_migration(migration_path):
                    self.logger.warning(
                        f"Verification failed for {migration_path.description}"
                    )

            self.logger.info("-" * 40)

        # Summary
        self.logger.info("=" * 60)
        self.logger.info("MIGRATION SUMMARY")
        self.logger.info(f"Total files migrated: {total_migrated}")
        self.logger.info(f"Total files failed: {total_failed}")
        self.logger.info(
            f"Overall status: {'SUCCESS' if total_success else 'PARTIAL FAILURE'}"
        )
        self.logger.info("=" * 60)

        return total_success


def main():
    parser = argparse.ArgumentParser(
        description="Migrate historical data to new bucket structure"
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "stg", "prod"],
        help="Environment to migrate (dev/stg/prod)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days back to migrate (default: 30)",
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
            f"Are you sure you want to migrate {args.days} days of data for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = HistoricalDataMigrator(args.env, args.days, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
