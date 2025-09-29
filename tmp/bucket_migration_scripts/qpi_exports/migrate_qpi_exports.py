#!/usr/bin/env python3
"""
QPI Exports Migration Script

This script migrates QPI export data from the old bucket structure to the new
dedicated export bucket structure.

Migration path:
- From: gs://data-bucket-{env}/QPI_exports/
- To: gs://de-bigquery-data-import-{env}/qpi_exports/

Usage:
    python3 migrate_qpi_exports.py --env prod --dry-run
    python3 migrate_qpi_exports.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from typing import List, Tuple, Dict


class QPIExportsMigrator:
    """Handles migration of QPI export data from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-import-{env}"
        self.old_path = f"gs://{self.old_bucket}/QPI_exports"
        self.new_path = f"gs://{self.new_bucket}/QPI_exports"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_qpi_exports_{env}_{timestamp}.log"

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

    def list_qpi_exports_contents(self) -> List[str]:
        """List all contents in the QPI_exports folder."""
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
                    self.logger.info("QPI_exports folder does not exist or is empty")
                    return []
                else:
                    self.logger.error(
                        f"Error listing QPI_exports contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning QPI_exports folder: {e}")
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

    def analyze_qpi_exports(self, files: List[str]) -> Dict[str, str]:
        """Analyze the QPI export files and provide insights."""
        analysis = {
            "total_files": str(len(files)),
            "file_types": {},
            "date_partitions": set(),
            "key_patterns": [],
        }

        for file_path in files:
            filename = file_path.split("/")[-1]

            # Extract date partitions (qpi_answers_YYYYMMDD pattern)
            if "qpi_answers_" in filename and filename.endswith(".jsonl"):
                # Extract date from filename
                parts = filename.replace("qpi_answers_", "").replace(".jsonl", "")
                if len(parts) == 8 and parts.isdigit():
                    analysis["date_partitions"].add(parts)

            # Count file types
            if filename.endswith(".jsonl"):
                analysis["file_types"]["jsonl"] = (
                    analysis["file_types"].get("jsonl", 0) + 1
                )
                analysis["key_patterns"].append(f"📄 {filename} (Daily export data)")
            else:
                extension = (
                    filename.split(".")[-1].lower() if "." in filename else "no_ext"
                )
                analysis["file_types"][extension] = (
                    analysis["file_types"].get(extension, 0) + 1
                )
                analysis["key_patterns"].append(f"📄 {filename}")

        # Convert date_partitions set to sorted list for display
        analysis["date_partitions"] = sorted(list(analysis["date_partitions"]))

        return analysis

    def migrate_qpi_exports(self) -> Tuple[bool, int, Dict]:
        """
        Migrate all QPI export files.

        Returns:
            Tuple of (success, files_count, analysis)
        """
        self.logger.info(f"Starting QPI exports migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_qpi_exports_contents()
        if not files:
            self.logger.info("No QPI export files to migrate")
            return True, 0, {}

        # Analyze files
        analysis = self.analyze_qpi_exports(files)
        self.logger.info(f"Found {len(files)} QPI export files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        # Log analysis
        self.logger.info("File analysis:")
        self.logger.info(f"  Total files: {analysis['total_files']}")
        self.logger.info(f"  File types: {analysis['file_types']}")

        if analysis["date_partitions"]:
            self.logger.info(
                f"  Date partitions found: {len(analysis['date_partitions'])}"
            )
            recent_dates = analysis["date_partitions"][-5:]  # Show last 5 dates
            self.logger.info(f"  Recent dates: {recent_dates}")

        if analysis["key_patterns"]:
            self.logger.info("  Sample files:")
            for pattern in analysis["key_patterns"][:5]:  # Show first 5
                self.logger.info(f"    {pattern}")

        if self.dry_run:
            self.logger.info("DRY RUN - QPI export files that would be migrated:")
            for file_path in files:
                relative_path = file_path.replace(self.old_path, "")
                # self.logger.info(f"  {file_path} → {self.new_path}{relative_path}")

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
                self.logger.info("Successfully migrated QPI export files")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files), analysis
            else:
                self.logger.error("QPI exports migration failed")
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

                    # Check for recent QPI export files
                    recent_jsonl_files = [f for f in new_files if f.endswith(".jsonl")]
                    if recent_jsonl_files:
                        self.logger.info(
                            f"✅ Found {len(recent_jsonl_files)} JSONL export files in new location"
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
        self.logger.info(f"Starting QPI Exports Migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info("⚠️  WARNING: This affects hot daily import pipeline!")
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
        success, files_migrated, analysis = self.migrate_qpi_exports()

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

        if analysis and analysis.get("date_partitions"):
            self.logger.info(f"Date partitions: {len(analysis['date_partitions'])}")

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        else:
            self.logger.info(
                "⚠️  IMPORTANT: Update code references to use new bucket path!"
            )
            self.logger.info("   Update orchestration/dags/jobs/import/import_qpi.py")
            self.logger.info(
                f"   Change path from: gs://data-bucket-{self.env}/QPI_exports/"
            )
            self.logger.info(
                f"   To: gs://de-bigquery-data-import-{self.env}/qpi_exports/"
            )

        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Migrate QPI exports to dedicated export bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  WARNING: This affects hot daily import pipeline!

This migration impacts:
- External QPI system daily exports
- orchestration/dags/jobs/import/import_qpi.py (daily processing)
- BigQuery raw dataset imports

Coordinate with development team before execution.
        """,
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
            f"⚠️  WARNING: This will migrate hot QPI export data to de-bigquery-data-import-{args.env}"
        )
        print(f"   This affects daily import pipeline!")
        print(f"   Source: gs://data-bucket-{args.env}/QPI_exports/")
        print(f"   Target: gs://de-bigquery-data-import-{args.env}/qpi_exports/")
        print()
        print("After migration, you MUST update code references:")
        print("   - orchestration/dags/jobs/import/import_qpi.py")
        print("   - QPI export path configurations")
        print()
        response = input(
            f"Are you sure you want to migrate QPI exports for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = QPIExportsMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
