#!/usr/bin/env python3
"""
Base32 Encode Utility Migration Script

This script migrates the base32-encode utility files from the old bucket structure
to the new dedicated shared utilities bucket.

Migration path:
- From: gs://data-bucket-{env}/base32-encode/
- To: gs://shared-utilities-bucket-{env}/base32-encode/

Usage:
    python migrate_base32_utils.py --env prod --dry-run
    python migrate_base32_utils.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple, Dict


class Base32UtilityMigrator:
    """Handles migration of base32-encode utility files from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"shared-utilities-bucket-{env}"
        self.old_path = f"gs://{self.old_bucket}/base32-encode/"
        self.new_path = f"gs://{self.new_bucket}/base32-encode/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_base32_utils_{env}_{timestamp}.log"

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

    def list_base32_contents(self) -> List[str]:
        """List all contents in the base32-encode folder."""
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
                    self.logger.info("base32-encode folder does not exist or is empty")
                    return []
                else:
                    self.logger.error(
                        f"Error listing base32-encode contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning base32-encode folder: {e}")
            return []

    def get_folder_size(self, path: str) -> str:
        """Get total size of folder using gcloud storage du."""
        try:
            result = subprocess.run(
                ["gcloud", "storage", "du", "--summarize", "--readable-sizes", path],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode == 0 and result.stdout:
                # Output format: "123.4 KiB  gs://bucket/path"
                size_info = result.stdout.strip().split()[0:2]
                return " ".join(size_info) if len(size_info) >= 2 else "unknown"
            else:
                return "unknown"

        except subprocess.SubprocessError:
            return "unknown"

    def analyze_utility_files(self, files: List[str]) -> Dict[str, str]:
        """Analyze the utility files and provide insights."""
        analysis = {
            "total_files": str(len(files)),
            "file_types": {},
            "key_files": [],
        }

        for file_path in files:
            filename = file_path.split("/")[-1]
            extension = filename.split(".")[-1].lower() if "." in filename else "no_ext"

            # Count file types
            if extension in analysis["file_types"]:
                analysis["file_types"][extension] += 1
            else:
                analysis["file_types"][extension] = 1

            # Identify key files
            if "base32.js" in filename:
                analysis["key_files"].append(
                    f"🔑 {filename} (Main base32 JavaScript utility)"
                )
            elif filename.endswith(".js"):
                analysis["key_files"].append(f"📜 {filename} (JavaScript file)")
            elif filename.endswith(".json"):
                analysis["key_files"].append(f"📋 {filename} (Configuration/Data file)")
            else:
                analysis["key_files"].append(f"📄 {filename}")

        return analysis

    def migrate_base32_utils(self) -> Tuple[bool, int, Dict]:
        """
        Migrate all base32-encode utility files.

        Returns:
            Tuple of (success, files_count, analysis)
        """
        self.logger.info(f"Starting base32-encode utility migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_base32_contents()
        if not files:
            self.logger.info("No base32-encode utility files to migrate")
            return True, 0, {}

        # Analyze files
        analysis = self.analyze_utility_files(files)
        self.logger.info(f"Found {len(files)} utility files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        # Log analysis
        self.logger.info("File analysis:")
        self.logger.info(f"  Total files: {analysis['total_files']}")
        self.logger.info(f"  File types: {analysis['file_types']}")

        if analysis["key_files"]:
            self.logger.info("  Key files:")
            for key_file in analysis["key_files"]:
                self.logger.info(f"    {key_file}")

        if self.dry_run:
            self.logger.info(
                "DRY RUN - base32-encode utility files that would be migrated:"
            )
            for file_path in files:
                relative_path = file_path.replace(self.old_path, "")
                self.logger.info(f"  {file_path} → {self.new_path}{relative_path}")

            return True, len(files), analysis

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
                self.logger.info("Successfully migrated base32-encode utility files")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files), analysis
            else:
                self.logger.error("base32-encode utility migration failed")
                self.logger.error(f"gcloud storage error: {result.stderr}")
                return False, 0, {}

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during migration: {e}")
            return False, 0, {}

    def verify_migration(self, original_file_count: int) -> bool:
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

                if len(new_files) >= original_file_count:
                    self.logger.info("✅ Migration verification successful")

                    # Check for key file (base32.js)
                    has_base32_js = any(
                        "base32.js" in file_path for file_path in new_files
                    )
                    if has_base32_js:
                        self.logger.info("✅ Key file base32.js found in new location")
                    else:
                        self.logger.warning(
                            "⚠️  Key file base32.js not found in new location"
                        )

                    return True
                else:
                    self.logger.warning(
                        f"⚠️  Migration verification warning: Expected {original_file_count} files, found {len(new_files)}"
                    )
                    return False
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
        self.logger.info(f"Starting Base32-Encode Utility Migration")
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
        success, files_migrated, analysis = self.migrate_base32_utils()

        if not success:
            self.logger.error("Migration failed")
            return False

        # Verify migration if not dry run
        if not self.dry_run and files_migrated > 0:
            if not self.verify_migration(files_migrated):
                self.logger.warning("Migration verification failed")

        # Summary
        self.logger.info("=" * 80)
        self.logger.info("MIGRATION SUMMARY")
        self.logger.info(f"Files migrated: {files_migrated}")
        self.logger.info(f"Status: {'SUCCESS' if success else 'FAILED'}")

        if analysis and "file_types" in analysis:
            self.logger.info("File type breakdown:")
            for file_type, count in analysis["file_types"].items():
                self.logger.info(f"  {file_type}: {count} files")

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        else:
            self.logger.info(
                "⚠️  IMPORTANT: Update code references to use new bucket path!"
            )
            self.logger.info("   Update orchestration/dags/common/config.py")
            self.logger.info(
                f"   Change path from: gs://data-bucket-{self.env}/base32-encode/"
            )
            self.logger.info(
                f"   To: gs://shared-utilities-bucket-{self.env}/base32-encode/"
            )

        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Migrate base32-encode utilities to shared utilities bucket"
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
            f"⚠️  WARNING: This will migrate base32-encode utilities to shared-utilities-bucket-{args.env}"
        )
        print(f"   This affects BigQuery UDF JavaScript functions!")
        print(f"   Source: gs://data-bucket-{args.env}/base32-encode/")
        print(f"   Target: gs://shared-utilities-bucket-{args.env}/base32-encode/")
        print()
        print("After migration, you MUST update code references:")
        print("   - orchestration/dags/common/config.py")
        print("   - BigQuery UDF JavaScript function paths")
        print()
        response = input(
            f"Are you sure you want to migrate base32-encode utilities for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = Base32UtilityMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
