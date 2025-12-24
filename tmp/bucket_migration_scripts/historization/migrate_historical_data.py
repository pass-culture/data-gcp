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
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple


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
            MigrationPath(
                old_folder="historization/tracking",
                new_bucket=f"de-bigquery-data-archive-{env}",
                new_folder="historization/tracking",
                description="Firebase tracking events",
            ),
            MigrationPath(
                old_folder="historization/int_firebase",
                new_bucket=f"de-bigquery-data-archive-{env}",
                new_folder="historization/int_firebase",
                description="Firebase intermediate events",
            ),
            MigrationPath(
                old_folder="historization/api_reco",
                new_bucket=f"ds-data-archive-{env}",
                new_folder="historization/api_reco",
                description="API recommendation data",
            ),
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
                ["gsutil", "ls", f"gs://{bucket_name}/"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.returncode == 0
        except subprocess.SubprocessError:
            return False

    def list_old_data(self, migration_path: MigrationPath) -> List[str]:
        """List existing data in the old bucket structure for given date range."""
        old_path = f"gs://{self.old_bucket}/{migration_path.old_folder}/"

        try:
            result = subprocess.run(
                ["gsutil", "ls", "-r", old_path],
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

            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    # Check if file contains any of our target dates
                    if any(date in line for date in date_strings):
                        files.append(line)

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
        self.logger.info(f"  From: gs://{self.old_bucket}/{migration_path.old_folder}/")
        self.logger.info(
            f"  To: gs://{migration_path.new_bucket}/{migration_path.new_folder}/"
        )

        # Check if target bucket exists
        if not self.check_bucket_exists(migration_path.new_bucket):
            self.logger.error(
                f"Target bucket gs://{migration_path.new_bucket}/ does not exist!"
            )
            return False, 0, 0

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
                    f"gs://{self.old_bucket}/{migration_path.old_folder}/", ""
                )
                new_path = f"gs://{migration_path.new_bucket}/{migration_path.new_folder}/{relative_path}"
                self.logger.info(f"  {file_path} → {new_path}")
            return True, len(files_to_migrate), 0

        # Perform actual migration using gsutil rsync for efficiency
        old_path = f"gs://{self.old_bucket}/{migration_path.old_folder}/"
        new_path = f"gs://{migration_path.new_bucket}/{migration_path.new_folder}/"

        # Use rsync with date-based inclusion filter
        date_strings = self.get_date_range()
        include_patterns = []
        for date in date_strings:
            include_patterns.extend(["-I", f"*{date}*"])

        try:
            # Build gsutil rsync command
            cmd = (
                ["gsutil", "-m", "rsync", "-r"]
                + include_patterns
                + [old_path, new_path]
            )

            self.logger.info(f"Executing: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode == 0:
                self.logger.info(
                    f"Successfully migrated data for {migration_path.description}"
                )
                self.logger.info(f"gsutil output: {result.stdout}")
                return True, len(files_to_migrate), 0
            else:
                self.logger.error(f"Migration failed for {migration_path.description}")
                self.logger.error(f"gsutil error: {result.stderr}")
                return False, 0, len(files_to_migrate)

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during migration: {e}")
            return False, 0, len(files_to_migrate)

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
                ["gsutil", "ls", "-r", new_path],
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
        self.logger.info("Starting historical data migration")
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
