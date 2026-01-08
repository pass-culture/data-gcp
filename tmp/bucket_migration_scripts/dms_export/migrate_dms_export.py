#!/usr/bin/env python3
"""
DMS Export Migration Script

This script migrates DMS export data from the old bucket structure to the new
dedicated export bucket structure.

Migration path:
- From: gs://data-bucket-{env}/dms_export/
- To: gs://de-bigquery-data-export-{env}/dms_export/

Usage:
    python migrate_dms_export.py --env prod --dry-run
    python migrate_dms_export.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Tuple


class DMSExportMigrator:
    """Handles migration of DMS export data from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-export-{env}"
        self.old_path = f"gs://{self.old_bucket}/dms_export/"
        self.new_path = f"gs://{self.new_bucket}/dms_export/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_dms_export_{env}_{timestamp}.log"

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

    def list_dms_export_contents(self) -> List[str]:
        """List all contents in the dms_export folder."""
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
                    self.logger.info("dms_export folder does not exist or is empty")
                    return []
                else:
                    self.logger.error(
                        f"Error listing dms_export contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning dms_export folder: {e}")
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

    def analyze_dms_export_files(self, files: List[str]) -> Dict[str, any]:
        """Analyze the DMS export files and provide insights."""
        analysis = {
            "total_files": len(files),
            "file_types": {},
            "staging_patterns": {"json_files": [], "parquet_files": []},
            "key_patterns": [],
        }

        for file_path in files:
            filename = file_path.split("/")[-1]
            extension = filename.split(".")[-1].lower() if "." in filename else "no_ext"

            # Count file types
            if extension in analysis["file_types"]:
                analysis["file_types"][extension] += 1
            else:
                analysis["file_types"][extension] = 1

            # Identify staging patterns
            if filename.startswith("unsorted_dms_") and filename.endswith(".json"):
                analysis["staging_patterns"]["json_files"].append(filename)
                analysis["key_patterns"].append(f"📄 {filename} (Raw DMS JSON)")
            elif filename.startswith("dms_") and filename.endswith(".parquet"):
                analysis["staging_patterns"]["parquet_files"].append(filename)
                analysis["key_patterns"].append(f"📊 {filename} (Processed Parquet)")
            elif filename.endswith(".json"):
                analysis["key_patterns"].append(f"📄 {filename} (JSON data)")
            elif filename.endswith(".parquet"):
                analysis["key_patterns"].append(f"📊 {filename} (Parquet data)")
            else:
                analysis["key_patterns"].append(f"📄 {filename}")

        return analysis

    def migrate_dms_export(self) -> Tuple[bool, int, Dict]:
        """
        Migrate all DMS export files.

        Returns:
            Tuple of (success, files_count, analysis)
        """
        self.logger.info("Starting DMS export migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_dms_export_contents()
        if not files:
            self.logger.info("No DMS export files to migrate")
            return True, 0, {}

        # Analyze files
        analysis = self.analyze_dms_export_files(files)
        self.logger.info(f"Found {len(files)} DMS export files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        # Log analysis
        self.logger.info("File analysis:")
        self.logger.info(f"  Total files: {analysis['total_files']}")
        self.logger.info(f"  File types: {analysis['file_types']}")

        # Show staging pipeline files
        json_count = len(analysis["staging_patterns"]["json_files"])
        parquet_count = len(analysis["staging_patterns"]["parquet_files"])

        if json_count > 0:
            self.logger.info(f"  Raw JSON files: {json_count}")
            recent_json = sorted(analysis["staging_patterns"]["json_files"])[-3:]
            for jf in recent_json:
                self.logger.info(f"    📄 {jf}")

        if parquet_count > 0:
            self.logger.info(f"  Processed Parquet files: {parquet_count}")
            recent_parquet = sorted(analysis["staging_patterns"]["parquet_files"])[-3:]
            for pf in recent_parquet:
                self.logger.info(f"    📊 {pf}")

        if self.dry_run:
            self.logger.info("DRY RUN - DMS export files that would be migrated:")
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
                self.logger.info("Successfully migrated DMS export files")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files), analysis
            else:
                self.logger.error("DMS export migration failed")
                self.logger.error(f"gcloud storage error: {result.stderr}")
                return False, 0, {}

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error during migration: {e}")
            return False, 0, {}

    def verify_migration(
        self, original_file_count: int, original_analysis: Dict
    ) -> bool:
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

                    # Verify critical staging files
                    new_json_files = [f for f in new_files if f.endswith(".json")]
                    new_parquet_files = [f for f in new_files if f.endswith(".parquet")]

                    original_json_count = len(
                        original_analysis.get("staging_patterns", {}).get(
                            "json_files", []
                        )
                    )
                    original_parquet_count = len(
                        original_analysis.get("staging_patterns", {}).get(
                            "parquet_files", []
                        )
                    )

                    if len(new_json_files) >= original_json_count:
                        self.logger.info(
                            f"✅ JSON staging files verified: {len(new_json_files)}"
                        )
                    if len(new_parquet_files) >= original_parquet_count:
                        self.logger.info(
                            f"✅ Parquet staging files verified: {len(new_parquet_files)}"
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
        self.logger.info("Starting DMS Export Migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info("⚠️  WARNING: This affects active ETL staging pipeline!")
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
        success, files_migrated, analysis = self.migrate_dms_export()

        if not success:
            self.logger.error("Migration failed")
            return False

        # Verify migration if not dry run
        if not self.dry_run and files_migrated > 0:
            if not self.verify_migration(files_migrated, analysis):
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

        if analysis and analysis.get("staging_patterns"):
            json_count = len(analysis["staging_patterns"]["json_files"])
            parquet_count = len(analysis["staging_patterns"]["parquet_files"])
            self.logger.info(
                f"Staging pipeline files: {json_count} JSON, {parquet_count} Parquet"
            )

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        else:
            self.logger.info(
                "⚠️  IMPORTANT: Update code references to use new bucket path!"
            )
            self.logger.info("   Update jobs/etl_jobs/external/dms/main.py")
            self.logger.info(
                "   Update jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py"
            )
            self.logger.info(
                "   Update orchestration/dags/jobs/import/import_dms_subscriptions.py"
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
        description="Migrate DMS export data to dedicated export bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  WARNING: This affects active ETL staging pipeline!

This migration impacts:
- jobs/etl_jobs/external/dms/main.py (writes JSON files)
- jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py (converts to Parquet)
- orchestration/dags/jobs/import/import_dms_subscriptions.py (daily processing)

Coordinate with development team for timing.
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
            f"⚠️  WARNING: This will migrate active DMS staging data to de-bigquery-data-export-{args.env}"
        )
        print("   This affects ETL staging pipeline!")
        print(f"   Source: gs://data-bucket-{args.env}/dms_export/")
        print(f"   Target: gs://de-bigquery-data-export-{args.env}/dms_export/")
        print()
        print("After migration, you MUST update code references:")
        print("   - jobs/etl_jobs/external/dms/main.py")
        print("   - jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py")
        print("   - orchestration/dags/jobs/import/import_dms_subscriptions.py")
        print()
        response = input(
            f"Are you sure you want to migrate DMS export data for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = DMSExportMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
