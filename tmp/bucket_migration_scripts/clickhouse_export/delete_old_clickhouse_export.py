#!/usr/bin/env python3
"""
ClickHouse Export Cleanup Script

This script safely deletes the old ClickHouse export folder after successful migration.
It includes multiple safety checks and requires explicit confirmation.

DANGER: This script permanently deletes active staging data!

Usage:
    python delete_old_clickhouse_export.py --env prod --dry-run
    python delete_old_clickhouse_export.py --env prod --confirm-delete
"""

import argparse
import json
import logging
import re
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Tuple


class ClickHouseExportCleanup:
    """Handles safe cleanup of old ClickHouse export data after successful migration."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-export-{env}"
        self.old_path = f"gs://{self.old_bucket}/clickhouse_export/"
        self.new_path = f"gs://{self.new_bucket}/clickhouse_export/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"delete_old_clickhouse_export_{env}_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

        # Manifest file to track what will be deleted
        self.manifest_file = (
            f"clickhouse_export_deletion_manifest_{env}_{timestamp}.json"
        )

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

    def list_old_clickhouse_contents(self) -> List[str]:
        """List all contents in the old ClickHouse export folder."""
        self.logger.info(f"Scanning contents of {self.old_path}")

        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", self.old_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                if (
                    "BucketNotFoundException" in result.stderr
                    or "not found" in result.stderr
                ):
                    self.logger.info(
                        "Old ClickHouse export folder does not exist or is already cleaned up"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing old ClickHouse export contents: {result.stderr}"
                    )
                    return []

            # Return all lines (files and directories)
            contents = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    contents.append(line)

            return contents

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning old ClickHouse export folder: {e}")
            return []

    def analyze_export_data_for_cleanup(self, contents: List[str]) -> Dict[str, any]:
        """Analyze export data to be deleted and provide insights."""
        files = [item for item in contents if not item.endswith("/")]

        analysis = {
            "total_files": len(files),
            "export_data_files": [],
            "env_paths_detected": set(),
            "date_patterns_detected": set(),
            "file_types": {},
        }

        # Pattern to extract environment and dates from export paths
        path_pattern = re.compile(r"clickhouse_export/([^/]+)/export/([^/]+)/")
        date_pattern = re.compile(r"(\d{8}|\d{4}-\d{2}-\d{2})")

        for file_path in files:
            filename = file_path.split("/")[-1]
            extension = filename.split(".")[-1].lower() if "." in filename else "no_ext"

            # Count file types
            if extension in analysis["file_types"]:
                analysis["file_types"][extension] += 1
            else:
                analysis["file_types"][extension] = 1

            # Identify export data files
            if any(
                ext in filename.lower() for ext in [".csv", ".parquet", ".json", ".gz"]
            ):
                analysis["export_data_files"].append(filename)

            # Extract path patterns
            path_match = path_pattern.search(file_path)
            if path_match:
                env_name = path_match.group(1)
                date_part = path_match.group(2)
                analysis["env_paths_detected"].add(env_name)
                analysis["date_patterns_detected"].add(date_part)

            # Additional date extraction
            date_match = date_pattern.search(file_path)
            if date_match:
                analysis["date_patterns_detected"].add(date_match.group(1))

        # Convert sets to sorted lists
        analysis["env_paths_detected"] = sorted(list(analysis["env_paths_detected"]))
        analysis["date_patterns_detected"] = sorted(
            list(analysis["date_patterns_detected"])
        )

        return analysis

    def verify_migration_success(self) -> Tuple[bool, Dict[str, str]]:
        """
        Verify that migration was successful by comparing old vs new data.

        Returns:
            Tuple of (migration_success, comparison_stats)
        """
        self.logger.info("Verifying migration success before cleanup...")

        # Check if new bucket exists
        if not self.check_bucket_exists(self.new_bucket):
            self.logger.error(f"New bucket {self.new_bucket} does not exist!")
            return False, {}

        # Get old data stats
        old_contents = self.list_old_clickhouse_contents()
        old_files = [item for item in old_contents if not item.endswith("/")]

        # Get new data stats
        try:
            result = subprocess.run(
                ["gcloud", "storage", "ls", "--recursive", self.new_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                self.logger.error(f"Cannot access new bucket {self.new_path}")
                return False, {}

            new_files = [
                line
                for line in result.stdout.strip().split("\n")
                if line and not line.endswith("/")
            ]

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error checking new bucket: {e}")
            return False, {}

        # Analyze export data files specifically
        old_export_files = [
            f
            for f in old_files
            if any(ext in f.lower() for ext in [".csv", ".parquet", ".json", ".gz"])
        ]
        new_export_files = [
            f
            for f in new_files
            if any(ext in f.lower() for ext in [".csv", ".parquet", ".json", ".gz"])
        ]

        stats = {
            "old_files": str(len(old_files)),
            "new_files": str(len(new_files)),
            "old_export_files": str(len(old_export_files)),
            "new_export_files": str(len(new_export_files)),
        }

        # Migration is successful if new bucket has at least as many files as old
        success = len(new_files) >= len(old_files) and len(old_files) > 0

        if success:
            self.logger.info("✅ Migration verification successful:")
            self.logger.info(
                f"   Old location: {len(old_files)} files ({len(old_export_files)} export data)"
            )
            self.logger.info(
                f"   New location: {len(new_files)} files ({len(new_export_files)} export data)"
            )

            if (
                len(new_export_files) >= len(old_export_files)
                and len(old_export_files) > 0
            ):
                self.logger.info("   ✅ Export data files verified")
        else:
            self.logger.error("❌ Migration verification failed:")
            self.logger.error(
                f"   Old location: {len(old_files)} files ({len(old_export_files)} export data)"
            )
            self.logger.error(
                f"   New location: {len(new_files)} files ({len(new_export_files)} export data)"
            )

            if len(old_files) == 0:
                self.logger.error(
                    "   No files found in old location - nothing to clean up"
                )
            elif len(new_files) < len(old_files):
                self.logger.error("   New location has fewer files than old location")

        return success, stats

    def create_deletion_manifest(self, contents_to_delete: List[str]) -> bool:
        """Create a manifest file listing everything that will be deleted."""
        cleanup_analysis = self.analyze_export_data_for_cleanup(contents_to_delete)

        manifest_data = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.env,
            "old_path": self.old_path,
            "new_path": self.new_path,
            "total_items": len(contents_to_delete),
            "files_to_delete": contents_to_delete,
            "export_analysis": {
                "total_export_files": len(cleanup_analysis["export_data_files"]),
                "file_types": cleanup_analysis["file_types"],
                "env_paths": cleanup_analysis["env_paths_detected"],
                "date_patterns": cleanup_analysis["date_patterns_detected"],
                "sample_export_files": cleanup_analysis["export_data_files"][
                    :10
                ],  # First 10 files
            },
            "warning": "This deletion affects active daily staging pipeline!",
        }

        try:
            with open(self.manifest_file, "w") as f:
                json.dump(manifest_data, f, indent=2)

            self.logger.info(f"📝 Created deletion manifest: {self.manifest_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create manifest file: {e}")
            return False

    def delete_old_clickhouse_export(self, contents_to_delete: List[str]) -> bool:
        """Delete the old ClickHouse export folder."""
        if not contents_to_delete:
            self.logger.info("No ClickHouse export data to delete")
            return True

        self.logger.info(
            f"Preparing to delete {len(contents_to_delete)} items from {self.old_path}"
        )

        # Analyze what's being deleted
        cleanup_analysis = self.analyze_export_data_for_cleanup(contents_to_delete)
        export_data_files = cleanup_analysis["export_data_files"]

        if export_data_files:
            self.logger.warning(
                f"⚠️  CRITICAL: Will delete {len(export_data_files)} export data files:"
            )
            # Show file type breakdown
            self.logger.warning(f"    📊 File types: {cleanup_analysis['file_types']}")

            # Show recent export files
            recent_export_files = sorted(export_data_files)[-5:]  # Show last 5
            for export_file in recent_export_files:
                self.logger.warning(f"    📄 {export_file}")

        # Show export patterns
        env_paths = cleanup_analysis["env_paths_detected"]
        date_patterns = cleanup_analysis["date_patterns_detected"]

        if env_paths:
            self.logger.warning(f"    🌍 Environment paths: {env_paths}")
        if date_patterns:
            self.logger.warning(f"    📅 Export dates: {len(date_patterns)} operations")
            if len(date_patterns) <= 5:
                self.logger.warning(f"    📅 Dates: {date_patterns}")
            else:
                self.logger.warning(f"    📅 Recent dates: {date_patterns[-5:]}")

        if self.dry_run:
            self.logger.info("DRY RUN - Items that would be deleted:")

            # Group by type for better display
            files = [item for item in contents_to_delete if not item.endswith("/")]
            directories = [item for item in contents_to_delete if item.endswith("/")]

            if directories:
                self.logger.info(f"  📁 Directories to delete: {len(directories)}")
                for directory in sorted(directories):
                    self.logger.info(f"    {directory}")

            self.logger.info(f"  📄 Files to delete: {len(files)}")
            for file_path in files[:10]:  # Show first 10 files
                filename = file_path.split("/")[-1]
                if any(
                    ext in filename.lower() for ext in [".csv", ".parquet", ".json"]
                ):
                    self.logger.info(f"    📊 {filename} (EXPORT DATA)")
                elif filename.endswith(".gz"):
                    self.logger.info(f"    📦 {filename} (COMPRESSED EXPORT)")
                else:
                    self.logger.info(f"    📄 {filename}")

            if len(files) > 10:
                self.logger.info(f"    ... and {len(files) - 10} more files")

            return True

        # Actual deletion using gcloud storage rm
        try:
            cmd = [
                "gcloud",
                "storage",
                "rm",
                "--recursive",
                self.old_path.rstrip("/"),  # Remove trailing slash for rm command
            ]

            self.logger.info(f"Executing: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode == 0:
                self.logger.info("✅ Successfully deleted old ClickHouse export data")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True
            else:
                self.logger.error("❌ Deletion failed")
                self.logger.error(f"gcloud storage error: {result.stderr}")
                return False

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during deletion: {e}")
            return False

    def run_cleanup(self) -> bool:
        """Run the complete cleanup process."""
        self.logger.info("=" * 80)
        self.logger.info("Starting ClickHouse Export Cleanup")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'DELETION'}")
        self.logger.info(f"Target: {self.old_path}")
        self.logger.info("⚠️  WARNING: This affects active daily staging pipeline!")
        self.logger.info("=" * 80)

        # Step 1: Verify migration success
        migration_success, stats = self.verify_migration_success()
        if not migration_success:
            self.logger.error("Migration verification failed - aborting cleanup")
            return False

        # Step 2: Get contents to delete
        contents_to_delete = self.list_old_clickhouse_contents()
        if not contents_to_delete:
            self.logger.info(
                "Nothing to clean up - old ClickHouse export data already removed"
            )
            return True

        # Step 3: Create deletion manifest
        if not self.create_deletion_manifest(contents_to_delete):
            self.logger.error("Failed to create deletion manifest - aborting")
            return False

        # Step 4: Delete old data
        success = self.delete_old_clickhouse_export(contents_to_delete)

        # Summary
        self.logger.info("=" * 80)
        self.logger.info("CLEANUP SUMMARY")
        self.logger.info(
            f"Migration verification: {'PASSED' if migration_success else 'FAILED'}"
        )
        self.logger.info(f"Items processed: {len(contents_to_delete)}")
        self.logger.info(f"Cleanup status: {'SUCCESS' if success else 'FAILED'}")
        self.logger.info(f"Manifest file: {self.manifest_file}")

        if stats:
            self.logger.info(
                f"Original files: {stats['old_files']} ({stats['old_export_files']} export data)"
            )
            self.logger.info(
                f"Migrated files: {stats['new_files']} ({stats['new_export_files']} export data)"
            )

        if self.dry_run:
            self.logger.info("DRY RUN - No actual deletion performed")
        else:
            if success:
                self.logger.info("⚠️  REMINDER: Update these code references:")
                self.logger.info(
                    "   - orchestration/dags/jobs/export/export_clickhouse.py"
                )
                self.logger.info(
                    f"   Change path from: gs://data-bucket-{self.env}/clickhouse_export/"
                )
                self.logger.info(
                    f"   To: gs://de-bigquery-data-export-{self.env}/clickhouse_export/"
                )

        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Safely delete old ClickHouse export data after successful migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DANGER: This script permanently deletes active staging data!

⚠️  WARNING: This affects active daily staging pipeline!

Safety Features:
- Verifies migration success before deletion
- Creates backup manifest of deleted items
- Requires explicit confirmation
- Dry-run mode by default

Only use after confirming:
1. Migration completed successfully
2. Code references updated in export_clickhouse.py
3. BigQuery → GCS → ClickHouse pipeline tested
4. External ClickHouse integration verified
        """,
    )

    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "stg", "prod"],
        help="Environment to clean up (dev/stg/prod)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Perform dry run without actual deletion (default)",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Execute actual deletion (overrides --dry-run)",
    )

    args = parser.parse_args()

    # If --confirm-delete is specified, disable dry-run
    dry_run = not args.confirm_delete

    if not dry_run:
        print("🚨 DANGER: This will PERMANENTLY DELETE ClickHouse export data! 🚨")
        print(f"   Environment: {args.env}")
        print(f"   Target: gs://data-bucket-{args.env}/clickhouse_export/")
        print("   ⚠️  This affects active daily staging pipeline!")
        print()
        print("Before proceeding, ensure that:")
        print("1. ✅ Migration completed successfully")
        print("2. ✅ Code references updated in export_clickhouse.py")
        print("3. ✅ BigQuery → GCS → ClickHouse pipeline tested")
        print("4. ✅ External ClickHouse integration verified")
        print("5. ✅ Backup manifest will be created")
        print()

        response = input("Type 'DELETE_CLICKHOUSE_EXPORT' to confirm deletion: ")
        if response != "DELETE_CLICKHOUSE_EXPORT":
            print("Deletion cancelled - confirmation text did not match.")
            return 1

        final_confirm = input(
            f"Final confirmation - delete ClickHouse export data from {args.env}? (yes/no): "
        )
        if final_confirm.lower() != "yes":
            print("Deletion cancelled.")
            return 1

    cleanup = ClickHouseExportCleanup(args.env, dry_run)
    success = cleanup.run_cleanup()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
