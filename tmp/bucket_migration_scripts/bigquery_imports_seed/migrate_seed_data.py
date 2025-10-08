#!/usr/bin/env python3
"""
BigQuery Imports Seed Data Migration Script

This script migrates all seed data from the old bucket structure to the new
dedicated seed data bucket.

Migration path:
- From: gs://data-bucket-{env}/bigquery_imports/seed/
- To: gs://de-bigquery-data-import-{env}/bigquery_imports/seed/

Usage:
    python migrate_seed_data.py --env prod --dry-run
    python migrate_seed_data.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple, Dict


class SeedDataMigrator:
    """Handles migration of seed data from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-import-{env}"
        self.old_path = f"gs://{self.old_bucket}/bigquery_imports/seed/"
        self.new_path = f"gs://{self.new_bucket}/bigquery_imports/seed/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_seed_data_{env}_{timestamp}.log"

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
                ["gcloud", "storage", "ls", f"gs://{bucket_name}/"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.returncode == 0
        except subprocess.SubprocessError:
            return False

    def list_seed_subfolders(self) -> List[str]:
        """List all subfolders in the seed directory."""
        self.logger.info(f"Scanning subfolders in {self.old_path}")

        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", self.old_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                if (
                    "BucketNotFoundException" in result.stderr
                    or "not found" in result.stderr
                ):
                    self.logger.info("Seed directory does not exist or is empty")
                    return []
                else:
                    self.logger.error(f"Error listing seed subfolders: {result.stderr}")
                    return []

            # Extract folder names (lines ending with '/')
            subfolders = []
            for line in result.stdout.strip().split("\n"):
                if line and line.endswith("/"):
                    folder_name = line.replace(self.old_path, "").rstrip("/")
                    if folder_name:  # Skip empty strings
                        subfolders.append(folder_name)

            return sorted(subfolders)

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning seed subfolders: {e}")
            return []

    def get_folder_contents(self, folder_path: str) -> List[str]:
        """Get all file contents of a specific folder."""
        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", folder_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):
                    files.append(line)

            return files

        except subprocess.SubprocessError:
            return []

    def get_folder_stats(self, folder_path: str) -> Dict[str, str]:
        """Get statistics for a folder (size and file count)."""
        files = self.get_folder_contents(folder_path)
        file_count = len(files)

        # Get size using gcloud storage du
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "storage",
                    "du",
                    "--summarize",
                    "--readable-sizes",
                    folder_path,
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0 and result.stdout:
                size_info = result.stdout.strip().split()[0:2]
                total_size = " ".join(size_info) if len(size_info) >= 2 else "unknown"
            else:
                total_size = "unknown"

        except subprocess.SubprocessError:
            total_size = "unknown"

        return {"file_count": str(file_count), "total_size": total_size}

    def migrate_seed_data(self) -> Tuple[bool, int, Dict[str, str]]:
        """
        Migrate all seed data using gcloud storage rsync.

        Returns:
            Tuple of (success, total_files_migrated, migration_stats)
        """
        self.logger.info(f"Starting seed data migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Get list of subfolders to migrate
        subfolders = self.list_seed_subfolders()
        if not subfolders:
            self.logger.info("No seed data subfolders to migrate")
            return True, 0, {}

        self.logger.info(f"Found {len(subfolders)} seed subfolders to migrate:")

        total_files = 0
        migration_stats = {}

        # Analyze each subfolder
        for subfolder in subfolders:
            folder_path = f"{self.old_path}{subfolder}/"
            stats = self.get_folder_stats(folder_path)
            migration_stats[subfolder] = stats
            total_files += int(stats["file_count"])

            self.logger.info(
                f"  📁 {subfolder}: {stats['file_count']} files, {stats['total_size']}"
            )

        self.logger.info(f"Total files across all subfolders: {total_files}")

        if self.dry_run:
            self.logger.info("DRY RUN - Seed data migration summary:")
            self.logger.info(f"  Source: {self.old_path}")
            self.logger.info(f"  Target: {self.new_path}")
            self.logger.info(f"  Subfolders to migrate: {len(subfolders)}")
            self.logger.info(f"  Total files: {total_files}")

            self.logger.info("Subfolder details:")
            for subfolder, stats in migration_stats.items():
                self.logger.info(
                    f"  - {subfolder}: {stats['file_count']} files ({stats['total_size']})"
                )

                # Show some sample files for the first few subfolders
                if (
                    subfolders.index(subfolder) < 3
                ):  # Show details for first 3 subfolders
                    folder_path = f"{self.old_path}{subfolder}/"
                    files = self.get_folder_contents(folder_path)
                    sample_files = files[:3] if len(files) > 3 else files

                    for file_path in sample_files:
                        relative_path = file_path.replace(self.old_path, "")
                        self.logger.info(
                            f"    {file_path} → {self.new_path}{relative_path}"
                        )

                    if len(files) > 3:
                        self.logger.info(f"    ... and {len(files) - 3} more files")

            return True, total_files, migration_stats

        # Perform actual migration using gcloud storage rsync
        try:
            cmd = [
                "gcloud",
                "storage",
                "rsync",
                "--recursive",
                "--exclude=.DS_Store",  # Exclude macOS metadata files
                self.old_path,
                self.new_path,
            ]

            self.logger.info(f"Executing: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode == 0:
                self.logger.info("Successfully migrated seed data")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, total_files, migration_stats
            else:
                self.logger.error("Seed data migration failed")
                self.logger.error(f"gcloud storage error: {result.stderr}")
                return False, 0, {}

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during migration: {e}")
            return False, 0, {}

    def verify_migration(self, original_stats: Dict[str, str]) -> bool:
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

                # Compare with original stats
                original_total = sum(
                    int(stats["file_count"]) for stats in original_stats.values()
                )

                if len(new_files) >= original_total:
                    self.logger.info("✅ Migration verification successful")
                    return len(new_files) > 0
                else:
                    self.logger.warning(
                        f"⚠️  Migration verification warning: Expected {original_total} files, found {len(new_files)}"
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
        self.logger.info("=" * 80)
        self.logger.info(f"Starting BigQuery Imports Seed Data Migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info("=" * 80)

        # Check if old bucket exists
        if not self.check_bucket_exists(self.old_bucket):
            self.logger.error(f"Source bucket gs://{self.old_bucket}/ does not exist!")
            return False

        # Check if new bucket exists
        if not self.check_bucket_exists(self.new_bucket):
            self.logger.error(f"Target bucket gs://{self.new_bucket}/ does not exist!")
            return False

        # Perform migration
        success, files_migrated, migration_stats = self.migrate_seed_data()

        if not success:
            self.logger.error("Migration failed")
            return False

        # Verify migration if not dry run
        if not self.dry_run and files_migrated > 0:
            if not self.verify_migration(migration_stats):
                self.logger.warning("Migration verification failed")

        # Summary
        self.logger.info("=" * 80)
        self.logger.info("MIGRATION SUMMARY")
        self.logger.info(f"Subfolders processed: {len(migration_stats)}")
        self.logger.info(f"Total files migrated: {files_migrated}")
        self.logger.info(f"Status: {'SUCCESS' if success else 'FAILED'}")

        if migration_stats:
            self.logger.info("Subfolder breakdown:")
            for subfolder, stats in migration_stats.items():
                self.logger.info(
                    f"  - {subfolder}: {stats['file_count']} files ({stats['total_size']})"
                )

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Migrate BigQuery Imports seed data to new bucket structure"
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
        print(
            f"⚠️  WARNING: This will migrate ALL seed data from bigquery_imports/seed/ to de-bigquery-data-import-{args.env}/bigquery_imports/seed/"
        )
        print(
            f"   This includes 50+ reference tables with CSV, Parquet, and Avro files"
        )
        print(f"   Source: gs://data-bucket-{args.env}/bigquery_imports/seed/")
        print(
            f"   Target: gs://de-bigquery-data-import-{args.env}/bigquery_imports/seed/"
        )
        print()
        response = input(
            f"Are you sure you want to migrate seed data for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = SeedDataMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
