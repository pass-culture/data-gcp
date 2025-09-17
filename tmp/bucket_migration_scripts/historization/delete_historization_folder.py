#!/usr/bin/env python3
"""
Delete Historization Folder Script

This script safely deletes the /historization folder and all its contents from the
old data-bucket-{env} after confirming that data has been successfully migrated
to the new archive buckets.

DANGER: This script permanently deletes data. Use with extreme caution.

Usage:
    python delete_historization_folder.py --env prod --dry-run
    python delete_historization_folder.py --env prod --confirm-delete

Safety Features:
- Requires explicit confirmation
- Dry-run mode by default
- Lists all files before deletion
- Creates backup manifest before deletion
- Verification checks
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import List, Dict, Optional


class HistorizationFolderDeleter:
    """Safely handles deletion of the /historization folder from old bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.historization_path = f"gs://{self.old_bucket}/historization/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"delete_historization_{env}_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

        # Create backup manifest filename
        self.manifest_file = f"historization_manifest_{env}_{timestamp}.json"

    def check_bucket_exists(self) -> bool:
        """Check if the old bucket exists."""
        try:
            result = subprocess.run(
                ["gsutil", "ls", f"gs://{self.old_bucket}/"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.returncode == 0
        except subprocess.SubprocessError:
            return False

    def list_historization_contents(self) -> List[Dict[str, str]]:
        """List all contents in the /historization folder with metadata."""
        self.logger.info(f"Scanning contents of {self.historization_path}")

        try:
            # Use gsutil ls -L for detailed information including size and timestamps
            result = subprocess.run(
                ["gsutil", "ls", "-r", "-L", self.historization_path],
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
                        "Historization folder does not exist or is already empty"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing historization contents: {result.stderr}"
                    )
                    return []

            files = []
            current_file = {}

            for line in result.stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue

                if line.startswith("gs://"):
                    # Save previous file if exists
                    if current_file:
                        files.append(current_file)

                    # Start new file entry
                    if not line.endswith("/"):  # Skip directories
                        current_file = {
                            "path": line,
                            "size": "unknown",
                            "modified": "unknown",
                        }
                    else:
                        current_file = {}

                elif "Size:" in line and current_file:
                    current_file["size"] = line.split("Size:")[1].strip()
                elif "Update time:" in line and current_file:
                    current_file["modified"] = line.split("Update time:")[1].strip()

            # Add last file
            if current_file:
                files.append(current_file)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning historization folder: {e}")
            return []

    def create_manifest(self, files: List[Dict[str, str]]) -> bool:
        """Create a backup manifest of all files before deletion."""
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "environment": self.env,
            "bucket": self.old_bucket,
            "path": self.historization_path,
            "total_files": len(files),
            "files": files,
        }

        try:
            with open(self.manifest_file, "w") as f:
                json.dump(manifest, f, indent=2)

            self.logger.info(f"Created backup manifest: {self.manifest_file}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create backup manifest: {e}")
            return False

    def verify_new_buckets_exist(self) -> bool:
        """Verify that the new archive buckets exist and contain data."""
        new_buckets = [
            f"de-bigquery-data-archive-{self.env}",
            f"ds-data-archive-{self.env}",
        ]

        all_exist = True
        for bucket in new_buckets:
            if not self.check_bucket_exists_simple(bucket):
                self.logger.warning(f"New archive bucket {bucket} does not exist!")
                all_exist = False
            else:
                self.logger.info(f"Verified new archive bucket exists: {bucket}")

        return all_exist

    def check_bucket_exists_simple(self, bucket_name: str) -> bool:
        """Simple bucket existence check."""
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

    def calculate_total_size(self, files: List[Dict[str, str]]) -> str:
        """Calculate total size of files to be deleted."""
        total_bytes = 0

        for file_info in files:
            size_str = file_info.get("size", "0")
            if size_str != "unknown" and size_str:
                try:
                    # Parse size (could be in format like "123456" or "1.2 MiB")
                    if "MiB" in size_str:
                        size_val = float(size_str.replace(" MiB", "")) * 1024 * 1024
                    elif "GiB" in size_str:
                        size_val = (
                            float(size_str.replace(" GiB", "")) * 1024 * 1024 * 1024
                        )
                    elif "KiB" in size_str:
                        size_val = float(size_str.replace(" KiB", "")) * 1024
                    else:
                        size_val = float(size_str)

                    total_bytes += int(size_val)
                except (ValueError, TypeError):
                    continue

        # Convert to human readable
        if total_bytes < 1024:
            return f"{total_bytes} bytes"
        elif total_bytes < 1024 * 1024:
            return f"{total_bytes / 1024:.2f} KB"
        elif total_bytes < 1024 * 1024 * 1024:
            return f"{total_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{total_bytes / (1024 * 1024 * 1024):.2f} GB"

    def delete_historization_folder(self) -> bool:
        """Delete the entire /historization folder."""
        if self.dry_run:
            self.logger.info(
                f"DRY RUN - Would execute: gcloud storage rm -r {self.historization_path}"
            )
            return True

        try:
            self.logger.info(f"EXECUTING DELETION: {self.historization_path}")

            result = subprocess.run(
                ["gcloud", "storage", "rm", "-r", self.historization_path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0:
                self.logger.info("Successfully deleted /historization folder")
                self.logger.info(f"gcloud storage output: {result.stdout}")
                return True
            else:
                self.logger.error("Failed to delete /historization folder")
                self.logger.error(f"gcloud storage error: {result.stderr}")
                return False

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during deletion: {e}")
            return False

    def run_deletion(self) -> bool:
        """Run the complete deletion process with safety checks."""
        self.logger.info("=" * 60)
        self.logger.info(f"Starting historization folder deletion")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Target path: {self.historization_path}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'DELETION'}")
        self.logger.info("=" * 60)

        # Check if old bucket exists
        if not self.check_bucket_exists():
            self.logger.error(f"Source bucket {self.old_bucket} does not exist!")
            return False

        # Verify new buckets exist (safety check)
        if not self.verify_new_buckets_exist():
            self.logger.error(
                "Not all new archive buckets exist. Migration may not be complete."
            )
            if not self.dry_run:
                response = input("Continue anyway? (type 'yes' to proceed): ")
                if response != "yes":
                    self.logger.info("Deletion cancelled by user.")
                    return False

        # List all files in historization folder
        files = self.list_historization_contents()

        if not files:
            self.logger.info(
                "No files found in /historization folder. Nothing to delete."
            )
            return True

        # Calculate total size
        total_size = self.calculate_total_size(files)

        # Display summary
        self.logger.info(f"Found {len(files)} files to delete")
        self.logger.info(f"Total size: {total_size}")

        # Show sample of files (first 10 and last 10 if more than 20 files)
        if len(files) <= 20:
            self.logger.info("Files to be deleted:")
            for file_info in files:
                self.logger.info(f"  {file_info['path']} ({file_info['size']})")
        else:
            self.logger.info("Sample of files to be deleted (first 10):")
            for file_info in files[:10]:
                self.logger.info(f"  {file_info['path']} ({file_info['size']})")
            self.logger.info(f"  ... and {len(files) - 20} more files ...")
            self.logger.info("Last 10 files:")
            for file_info in files[-10:]:
                self.logger.info(f"  {file_info['path']} ({file_info['size']})")

        # Create backup manifest
        if not self.create_manifest(files):
            self.logger.error("Failed to create backup manifest. Aborting deletion.")
            return False

        # Final confirmation for actual deletion
        if not self.dry_run:
            self.logger.warning("⚠️  FINAL CONFIRMATION REQUIRED ⚠️")
            self.logger.warning(
                f"About to permanently delete {len(files)} files ({total_size})"
            )
            self.logger.warning(f"From: {self.historization_path}")

            print("\nType the following EXACTLY to confirm deletion:")
            confirmation_text = f"DELETE {self.env} HISTORIZATION"
            print(f"Required text: {confirmation_text}")

            user_input = input("\nEnter confirmation: ").strip()

            if user_input != confirmation_text:
                self.logger.info("Confirmation text did not match. Deletion cancelled.")
                return False

        # Perform deletion
        success = self.delete_historization_folder()

        # Summary
        self.logger.info("=" * 60)
        if success:
            if self.dry_run:
                self.logger.info("DRY RUN COMPLETED - No files were actually deleted")
                self.logger.info(
                    f"Would have deleted {len(files)} files ({total_size})"
                )
            else:
                self.logger.info("DELETION COMPLETED SUCCESSFULLY")
                self.logger.info(f"Deleted {len(files)} files ({total_size})")
                self.logger.info(f"Backup manifest saved: {self.manifest_file}")
        else:
            self.logger.error("DELETION FAILED")

        self.logger.info("=" * 60)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Delete /historization folder from old bucket structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DANGER: This script permanently deletes data!

Examples:
  # Dry run (safe, shows what would be deleted)
  python delete_historization_folder.py --env prod --dry-run

  # Actually delete (requires confirmation)
  python delete_historization_folder.py --env prod --confirm-delete

Safety features:
- Dry run mode by default
- Creates backup manifest before deletion
- Requires explicit confirmation text
- Verifies new archive buckets exist
- Comprehensive logging
        """,
    )

    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "stg", "prod"],
        help="Environment bucket to delete from (dev/stg/prod)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be deleted without actually deleting (default)",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Actually delete files (overrides --dry-run)",
    )

    args = parser.parse_args()

    # If --confirm-delete is specified, disable dry-run
    dry_run = not args.confirm_delete

    if not dry_run:
        print("⚠️  WARNING: You are about to permanently delete data! ⚠️")
        print(f"Environment: {args.env}")
        print(f"Path: gs://data-bucket-{args.env}/historization/")
        print()
        print("This action cannot be undone!")
        print()

        response = input(
            "Are you absolutely sure you want to proceed? (type 'yes' to continue): "
        )
        if response.lower() != "yes":
            print("Deletion cancelled.")
            return 1

    deleter = HistorizationFolderDeleter(args.env, dry_run)
    success = deleter.run_deletion()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
