#!/usr/bin/env python3
"""
CloudSQL Recommendation Sync Cleanup Script

This script safely deletes the old recommendation sync folder after successful migration.
It includes multiple safety checks and requires explicit confirmation.

DANGER: This script permanently deletes active sync data!

Usage:
    python delete_old_recommendation_sync.py --env prod --dry-run
    python delete_old_recommendation_sync.py --env prod --confirm-delete
"""

import argparse
import json
import logging
import re
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Tuple


class RecommendationSyncCleanup:
    """Handles safe cleanup of old recommendation sync data after successful migration."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-export-{env}"
        self.old_path = (
            f"gs://{self.old_bucket}/export/cloudsql_recommendation_tables_to_bigquery/"
        )
        self.new_path = f"gs://{self.new_bucket}/export_cloudsql_recommendation/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"delete_old_recommendation_sync_{env}_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

        # Manifest file to track what will be deleted
        self.manifest_file = (
            f"recommendation_sync_deletion_manifest_{env}_{timestamp}.json"
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

    def list_old_recommendation_contents(self) -> List[str]:
        """List all contents in the old recommendation sync folder."""
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
                        "Old recommendation sync folder does not exist or is already cleaned up"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing old recommendation sync contents: {result.stderr}"
                    )
                    return []

            # Return all lines (files and directories)
            contents = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    contents.append(line)

            return contents

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning old recommendation sync folder: {e}")
            return []

    def analyze_sync_data_for_cleanup(self, contents: List[str]) -> Dict[str, any]:
        """Analyze sync data to be deleted and provide insights."""
        files = [item for item in contents if not item.endswith("/")]

        analysis = {
            "total_files": len(files),
            "sync_data_files": [],
            "timestamps_detected": set(),
            "file_types": {},
        }

        # Pattern to extract timestamps from sync files/folders
        timestamp_pattern = re.compile(r"(\d{8}[_T]\d{6}|\d{14}|\d{10})")

        for file_path in files:
            filename = file_path.split("/")[-1]
            extension = filename.split(".")[-1].lower() if "." in filename else "no_ext"

            # Count file types
            if extension in analysis["file_types"]:
                analysis["file_types"][extension] += 1
            else:
                analysis["file_types"][extension] = 1

            # Identify sync data files
            if any(ext in filename.lower() for ext in [".csv", ".parquet", ".json"]):
                analysis["sync_data_files"].append(filename)

            # Extract timestamp information
            timestamp_match = timestamp_pattern.search(file_path)
            if timestamp_match:
                analysis["timestamps_detected"].add(timestamp_match.group(1))

        # Convert timestamps set to sorted list
        analysis["timestamps_detected"] = sorted(list(analysis["timestamps_detected"]))

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
        old_contents = self.list_old_recommendation_contents()
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

        # Analyze sync data files specifically
        old_sync_files = [
            f
            for f in old_files
            if any(ext in f.lower() for ext in [".csv", ".parquet", ".json"])
        ]
        new_sync_files = [
            f
            for f in new_files
            if any(ext in f.lower() for ext in [".csv", ".parquet", ".json"])
        ]

        stats = {
            "old_files": str(len(old_files)),
            "new_files": str(len(new_files)),
            "old_sync_files": str(len(old_sync_files)),
            "new_sync_files": str(len(new_sync_files)),
        }

        # Migration is successful if new bucket has at least as many files as old
        success = len(new_files) >= len(old_files) and len(old_files) > 0

        if success:
            self.logger.info("✅ Migration verification successful:")
            self.logger.info(
                f"   Old location: {len(old_files)} files ({len(old_sync_files)} sync data)"
            )
            self.logger.info(
                f"   New location: {len(new_files)} files ({len(new_sync_files)} sync data)"
            )

            if len(new_sync_files) >= len(old_sync_files) and len(old_sync_files) > 0:
                self.logger.info("   ✅ Sync data files verified")
        else:
            self.logger.error("❌ Migration verification failed:")
            self.logger.error(
                f"   Old location: {len(old_files)} files ({len(old_sync_files)} sync data)"
            )
            self.logger.error(
                f"   New location: {len(new_files)} files ({len(new_sync_files)} sync data)"
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
        cleanup_analysis = self.analyze_sync_data_for_cleanup(contents_to_delete)

        manifest_data = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.env,
            "old_path": self.old_path,
            "new_path": self.new_path,
            "total_items": len(contents_to_delete),
            "files_to_delete": contents_to_delete,
            "sync_analysis": {
                "total_sync_files": len(cleanup_analysis["sync_data_files"]),
                "file_types": cleanup_analysis["file_types"],
                "timestamps_detected": cleanup_analysis["timestamps_detected"],
                "sample_sync_files": cleanup_analysis["sync_data_files"][
                    :10
                ],  # First 10 files
            },
            "warning": "This deletion affects active bidirectional sync operations!",
        }

        try:
            with open(self.manifest_file, "w") as f:
                json.dump(manifest_data, f, indent=2)

            self.logger.info(f"📝 Created deletion manifest: {self.manifest_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create manifest file: {e}")
            return False

    def delete_old_recommendation_sync(self, contents_to_delete: List[str]) -> bool:
        """Delete the old recommendation sync folder."""
        if not contents_to_delete:
            self.logger.info("No recommendation sync data to delete")
            return True

        self.logger.info(
            f"Preparing to delete {len(contents_to_delete)} items from {self.old_path}"
        )

        # Analyze what's being deleted
        cleanup_analysis = self.analyze_sync_data_for_cleanup(contents_to_delete)
        sync_data_files = cleanup_analysis["sync_data_files"]

        if sync_data_files:
            self.logger.warning(
                f"⚠️  CRITICAL: Will delete {len(sync_data_files)} sync data files:"
            )
            # Show file type breakdown
            self.logger.warning(f"    📊 File types: {cleanup_analysis['file_types']}")

            # Show recent sync files
            recent_sync_files = sorted(sync_data_files)[-5:]  # Show last 5
            for sync_file in recent_sync_files:
                self.logger.warning(f"    📄 {sync_file}")

        # Show timestamp coverage
        timestamps = cleanup_analysis["timestamps_detected"]
        if timestamps:
            self.logger.warning(f"    🕒 Sync timestamps: {len(timestamps)} operations")
            if len(timestamps) <= 5:
                self.logger.warning(f"    🕒 Timestamps: {timestamps}")
            else:
                self.logger.warning(f"    🕒 Recent timestamps: {timestamps[-5:]}")

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
                    self.logger.info(f"    📊 {filename} (SYNC DATA)")
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
                self.logger.info("✅ Successfully deleted old recommendation sync data")
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
        self.logger.info("Starting CloudSQL Recommendation Sync Cleanup")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'DELETION'}")
        self.logger.info(f"Target: {self.old_path}")
        self.logger.info(
            "⚠️  WARNING: This affects active bidirectional sync operations!"
        )
        self.logger.info("=" * 80)

        # Step 1: Verify migration success
        migration_success, stats = self.verify_migration_success()
        if not migration_success:
            self.logger.error("Migration verification failed - aborting cleanup")
            return False

        # Step 2: Get contents to delete
        contents_to_delete = self.list_old_recommendation_contents()
        if not contents_to_delete:
            self.logger.info(
                "Nothing to clean up - old recommendation sync data already removed"
            )
            return True

        # Step 3: Create deletion manifest
        if not self.create_deletion_manifest(contents_to_delete):
            self.logger.error("Failed to create deletion manifest - aborting")
            return False

        # Step 4: Delete old data
        success = self.delete_old_recommendation_sync(contents_to_delete)

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
                f"Original files: {stats['old_files']} ({stats['old_sync_files']} sync data)"
            )
            self.logger.info(
                f"Migrated files: {stats['new_files']} ({stats['new_sync_files']} sync data)"
            )

        if self.dry_run:
            self.logger.info("DRY RUN - No actual deletion performed")
        else:
            if success:
                self.logger.info("⚠️  REMINDER: Update these code references:")
                self.logger.info(
                    "   - orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py"
                )
                self.logger.info(
                    f"   Change path from: gs://data-bucket-{self.env}/export/cloudsql_recommendation_tables_to_bigquery/"
                )
                self.logger.info(
                    f"   To: gs://de-bigquery-data-export-{self.env}/export_cloudsql_recommendation/"
                )

        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Safely delete old recommendation sync data after successful migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DANGER: This script permanently deletes active sync data!

⚠️  WARNING: This affects active bidirectional sync operations!

Safety Features:
- Verifies migration success before deletion
- Creates backup manifest of deleted items
- Requires explicit confirmation
- Dry-run mode by default

Only use after confirming:
1. Migration completed successfully
2. Code references updated in sync DAG
3. Bidirectional sync tested with new bucket
4. Sync coordination operations verified
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
        print("🚨 DANGER: This will PERMANENTLY DELETE recommendation sync data! 🚨")
        print(f"   Environment: {args.env}")
        print(
            f"   Target: gs://data-bucket-{args.env}/export/cloudsql_recommendation_tables_to_bigquery/"
        )
        print("   ⚠️  This affects active bidirectional sync operations!")
        print()
        print("Before proceeding, ensure that:")
        print("1. ✅ Migration completed successfully")
        print("2. ✅ Code references updated in sync DAG")
        print("3. ✅ Bidirectional sync tested with new bucket")
        print("4. ✅ Sync coordination operations verified")
        print("5. ✅ Backup manifest will be created")
        print()

        response = input("Type 'DELETE_RECOMMENDATION_SYNC' to confirm deletion: ")
        if response != "DELETE_RECOMMENDATION_SYNC":
            print("Deletion cancelled - confirmation text did not match.")
            return 1

        final_confirm = input(
            f"Final confirmation - delete recommendation sync data from {args.env}? (yes/no): "
        )
        if final_confirm.lower() != "yes":
            print("Deletion cancelled.")
            return 1

    cleanup = RecommendationSyncCleanup(args.env, dry_run)
    success = cleanup.run_cleanup()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
