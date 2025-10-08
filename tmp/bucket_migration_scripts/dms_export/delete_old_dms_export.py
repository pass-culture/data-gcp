#!/usr/bin/env python3
"""
DMS Export Cleanup Script

This script safely deletes the old DMS export folder after successful migration.
It includes multiple safety checks and requires explicit confirmation.

DANGER: This script permanently deletes active staging data!

Usage:
    python delete_old_dms_export.py --env prod --dry-run
    python delete_old_dms_export.py --env prod --confirm-delete
"""

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime
from typing import List, Dict, Tuple


class DMSExportCleanup:
    """Handles safe cleanup of old DMS export data after successful migration."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-export-{env}"
        self.old_path = f"gs://{self.old_bucket}/dms_export/"
        self.new_path = f"gs://{self.new_bucket}/dms_export/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"delete_old_dms_export_{env}_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

        # Manifest file to track what will be deleted
        self.manifest_file = f"dms_export_deletion_manifest_{env}_{timestamp}.json"

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

    def list_old_dms_contents(self) -> List[str]:
        """List all contents in the old DMS export folder."""
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
                        "Old DMS export folder does not exist or is already cleaned up"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing old DMS export contents: {result.stderr}"
                    )
                    return []

            # Return all lines (files and directories)
            contents = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    contents.append(line)

            return contents

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning old DMS export folder: {e}")
            return []

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
        old_contents = self.list_old_dms_contents()
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

        # Analyze staging pipeline files
        old_json_files = [f for f in old_files if f.endswith(".json")]
        old_parquet_files = [f for f in old_files if f.endswith(".parquet")]
        new_json_files = [f for f in new_files if f.endswith(".json")]
        new_parquet_files = [f for f in new_files if f.endswith(".parquet")]

        stats = {
            "old_files": str(len(old_files)),
            "new_files": str(len(new_files)),
            "old_json_files": str(len(old_json_files)),
            "new_json_files": str(len(new_json_files)),
            "old_parquet_files": str(len(old_parquet_files)),
            "new_parquet_files": str(len(new_parquet_files)),
        }

        # Migration is successful if new bucket has at least as many files as old
        success = len(new_files) >= len(old_files) and len(old_files) > 0

        if success:
            self.logger.info(f"✅ Migration verification successful:")
            self.logger.info(
                f"   Old location: {len(old_files)} files ({len(old_json_files)} JSON, {len(old_parquet_files)} Parquet)"
            )
            self.logger.info(
                f"   New location: {len(new_files)} files ({len(new_json_files)} JSON, {len(new_parquet_files)} Parquet)"
            )

            if len(new_json_files) >= len(old_json_files) and len(old_json_files) > 0:
                self.logger.info(f"   ✅ JSON staging files verified")
            if (
                len(new_parquet_files) >= len(old_parquet_files)
                and len(old_parquet_files) > 0
            ):
                self.logger.info(f"   ✅ Parquet staging files verified")
        else:
            self.logger.error(f"❌ Migration verification failed:")
            self.logger.error(
                f"   Old location: {len(old_files)} files ({len(old_json_files)} JSON, {len(old_parquet_files)} Parquet)"
            )
            self.logger.error(
                f"   New location: {len(new_files)} files ({len(new_json_files)} JSON, {len(new_parquet_files)} Parquet)"
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
        staging_files = {
            "json_files": [f for f in contents_to_delete if f.endswith(".json")],
            "parquet_files": [f for f in contents_to_delete if f.endswith(".parquet")],
        }

        manifest_data = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.env,
            "old_path": self.old_path,
            "new_path": self.new_path,
            "total_items": len(contents_to_delete),
            "files_to_delete": contents_to_delete,
            "staging_files": staging_files,
            "warning": "This deletion affects active ETL staging pipeline!",
        }

        try:
            with open(self.manifest_file, "w") as f:
                json.dump(manifest_data, f, indent=2)

            self.logger.info(f"📝 Created deletion manifest: {self.manifest_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create manifest file: {e}")
            return False

    def delete_old_dms_export(self, contents_to_delete: List[str]) -> bool:
        """Delete the old DMS export folder."""
        if not contents_to_delete:
            self.logger.info("No DMS export data to delete")
            return True

        self.logger.info(
            f"Preparing to delete {len(contents_to_delete)} items from {self.old_path}"
        )

        # Identify critical staging files
        files_to_delete = [
            item for item in contents_to_delete if not item.endswith("/")
        ]
        json_staging_files = [f for f in files_to_delete if f.endswith(".json")]
        parquet_staging_files = [f for f in files_to_delete if f.endswith(".parquet")]

        if json_staging_files:
            self.logger.warning(
                f"⚠️  CRITICAL: Will delete {len(json_staging_files)} JSON staging files:"
            )
            # Show recent files
            recent_json = sorted(json_staging_files)[-3:]  # Show last 3
            for jf in recent_json:
                filename = jf.split("/")[-1]
                self.logger.warning(f"    📄 {filename}")

        if parquet_staging_files:
            self.logger.warning(
                f"⚠️  CRITICAL: Will delete {len(parquet_staging_files)} Parquet staging files:"
            )
            # Show recent files
            recent_parquet = sorted(parquet_staging_files)[-3:]  # Show last 3
            for pf in recent_parquet:
                filename = pf.split("/")[-1]
                self.logger.warning(f"    📊 {filename}")

        if self.dry_run:
            self.logger.info("DRY RUN - Items that would be deleted:")

            # Group by type for better display
            files = [item for item in contents_to_delete if not item.endswith("/")]
            directories = [item for item in contents_to_delete if item.endswith("/")]

            if directories:
                self.logger.info(f"  📁 Directories to delete: {len(directories)}")
                for directory in directories:
                    self.logger.info(f"    {directory}")

            self.logger.info(f"  📄 Files to delete: {len(files)}")
            for file_path in files[:10]:  # Show first 10 files
                filename = file_path.split("/")[-1]
                if filename.endswith(".json"):
                    self.logger.info(f"    📄 {filename} (JSON STAGING)")
                elif filename.endswith(".parquet"):
                    self.logger.info(f"    📊 {filename} (PARQUET STAGING)")
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
                self.logger.info("✅ Successfully deleted old DMS export data")
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
        self.logger.info(f"Starting DMS Export Cleanup")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'DELETION'}")
        self.logger.info(f"Target: {self.old_path}")
        self.logger.info("⚠️  WARNING: This affects active ETL staging pipeline!")
        self.logger.info("=" * 80)

        # Step 1: Verify migration success
        migration_success, stats = self.verify_migration_success()
        if not migration_success:
            self.logger.error("Migration verification failed - aborting cleanup")
            return False

        # Step 2: Get contents to delete
        contents_to_delete = self.list_old_dms_contents()
        if not contents_to_delete:
            self.logger.info(
                "Nothing to clean up - old DMS export data already removed"
            )
            return True

        # Step 3: Create deletion manifest
        if not self.create_deletion_manifest(contents_to_delete):
            self.logger.error("Failed to create deletion manifest - aborting")
            return False

        # Step 4: Delete old data
        success = self.delete_old_dms_export(contents_to_delete)

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
                f"Original files: {stats['old_files']} ({stats['old_json_files']} JSON, {stats['old_parquet_files']} Parquet)"
            )
            self.logger.info(
                f"Migrated files: {stats['new_files']} ({stats['new_json_files']} JSON, {stats['new_parquet_files']} Parquet)"
            )

        if self.dry_run:
            self.logger.info("DRY RUN - No actual deletion performed")
        else:
            if success:
                self.logger.info("⚠️  REMINDER: Update these code references:")
                self.logger.info("   - jobs/etl_jobs/external/dms/main.py")
                self.logger.info(
                    "   - jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py"
                )
                self.logger.info(
                    "   - orchestration/dags/jobs/import/import_dms_subscriptions.py"
                )
                self.logger.info(
                    f"   Change paths from: gs://data-bucket-{self.env}/dms_export/"
                )
                self.logger.info(
                    f"   To: gs://de-bigquery-data-export-{self.env}/dms_export/"
                )

        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Safely delete old DMS export data after successful migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DANGER: This script permanently deletes active staging data!

⚠️  WARNING: This affects active ETL staging pipeline!

Safety Features:
- Verifies migration success before deletion
- Creates backup manifest of deleted items
- Requires explicit confirmation
- Dry-run mode by default

Only use after confirming:
1. Migration completed successfully
2. Code references updated in all DMS ETL jobs
3. ETL pipeline tested with new bucket
4. Staging operations verified end-to-end
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
        print("🚨 DANGER: This will PERMANENTLY DELETE DMS staging data! 🚨")
        print(f"   Environment: {args.env}")
        print(f"   Target: gs://data-bucket-{args.env}/dms_export/")
        print("   ⚠️  This affects active ETL staging pipeline!")
        print()
        print("Before proceeding, ensure that:")
        print("1. ✅ Migration completed successfully")
        print("2. ✅ Code references updated in all DMS ETL jobs")
        print("3. ✅ ETL pipeline tested with new bucket")
        print("4. ✅ JSON → Parquet → BigQuery flow verified")
        print("5. ✅ Backup manifest will be created")
        print()

        response = input("Type 'DELETE_DMS_EXPORT' to confirm deletion: ")
        if response != "DELETE_DMS_EXPORT":
            print("Deletion cancelled - confirmation text did not match.")
            return 1

        final_confirm = input(
            f"Final confirmation - delete DMS staging data from {args.env}? (yes/no): "
        )
        if final_confirm.lower() != "yes":
            print("Deletion cancelled.")
            return 1

    cleanup = DMSExportCleanup(args.env, dry_run)
    success = cleanup.run_cleanup()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
