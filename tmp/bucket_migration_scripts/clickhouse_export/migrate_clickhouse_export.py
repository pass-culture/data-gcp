#!/usr/bin/env python3
"""
ClickHouse Export Migration Script

This script migrates ClickHouse export data from the old bucket structure to the new
dedicated export bucket structure.

Migration path:
- From: gs://data-bucket-{env}/clickhouse_export/
- To: gs://de-bigquery-data-export-{env}/clickhouse_export/

Usage:
    python migrate_clickhouse_export.py --env prod --dry-run
    python migrate_clickhouse_export.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import re


class ClickHouseExportMigrator:
    """Handles migration of ClickHouse export data from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-export-{env}"
        self.old_path = f"gs://{self.old_bucket}/clickhouse_export/"
        self.new_path = f"gs://{self.new_bucket}/clickhouse_export/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_clickhouse_export_{env}_{timestamp}.log"

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

    def list_clickhouse_export_contents(self) -> List[str]:
        """List all contents in the clickhouse_export folder."""
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
                        "clickhouse_export folder does not exist or is empty"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing clickhouse_export contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning clickhouse_export folder: {e}")
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

    def analyze_clickhouse_export_files(self, files: List[str]) -> Dict[str, any]:
        """Analyze the ClickHouse export files and provide insights."""
        analysis = {
            "total_files": len(files),
            "file_types": {},
            "export_patterns": {"env_paths": set(), "date_patterns": set()},
            "staging_patterns": [],
        }

        # Pattern to extract environment and dates from export paths
        # Expected pattern: clickhouse_export/{env}/export/{DATE}/
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

            # Extract export path patterns
            path_match = path_pattern.search(file_path)
            if path_match:
                env_name = path_match.group(1)
                date_part = path_match.group(2)
                analysis["export_patterns"]["env_paths"].add(env_name)
                analysis["export_patterns"]["date_patterns"].add(date_part)

            # Additional date pattern extraction
            date_match = date_pattern.search(file_path)
            if date_match:
                date_str = date_match.group(1)
                analysis["export_patterns"]["date_patterns"].add(date_str)

            # Identify staging patterns
            if "export" in file_path.lower():
                if filename.endswith((".csv", ".parquet", ".json")):
                    analysis["staging_patterns"].append(
                        f"📊 {filename} (Export data file)"
                    )
                elif filename.endswith(".gz"):
                    analysis["staging_patterns"].append(
                        f"📦 {filename} (Compressed export)"
                    )
                else:
                    analysis["staging_patterns"].append(f"📄 {filename} (Export file)")
            else:
                analysis["staging_patterns"].append(f"📄 {filename}")

        # Convert sets to sorted lists for display
        analysis["export_patterns"]["env_paths"] = sorted(
            list(analysis["export_patterns"]["env_paths"])
        )
        analysis["export_patterns"]["date_patterns"] = sorted(
            list(analysis["export_patterns"]["date_patterns"])
        )

        return analysis

    def migrate_clickhouse_export(self) -> Tuple[bool, int, Dict]:
        """
        Migrate all ClickHouse export files.

        Returns:
            Tuple of (success, files_count, analysis)
        """
        self.logger.info(f"Starting ClickHouse export migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_clickhouse_export_contents()
        if not files:
            self.logger.info("No ClickHouse export files to migrate")
            return True, 0, {}

        # Analyze files
        analysis = self.analyze_clickhouse_export_files(files)
        self.logger.info(f"Found {len(files)} ClickHouse export files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        # Log analysis
        self.logger.info("File analysis:")
        self.logger.info(f"  Total files: {analysis['total_files']}")
        self.logger.info(f"  File types: {analysis['file_types']}")

        # Show export patterns
        env_paths = analysis["export_patterns"]["env_paths"]
        date_patterns = analysis["export_patterns"]["date_patterns"]

        if env_paths:
            self.logger.info(f"  Environment paths: {env_paths}")

        if date_patterns:
            self.logger.info(f"  Export dates detected: {len(date_patterns)}")
            recent_dates = sorted(date_patterns)[-5:]  # Show last 5
            self.logger.info(f"  Recent export dates: {recent_dates}")

        if analysis["staging_patterns"]:
            self.logger.info("  Sample export files:")
            for pattern in analysis["staging_patterns"][:5]:  # Show first 5
                self.logger.info(f"    {pattern}")

        if self.dry_run:
            self.logger.info(
                "DRY RUN - ClickHouse export files that would be migrated:"
            )
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
                self.logger.info("Successfully migrated ClickHouse export files")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files), analysis
            else:
                self.logger.error("ClickHouse export migration failed")
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

                    # Verify export data files
                    new_export_files = [
                        f
                        for f in new_files
                        if any(
                            ext in f.lower()
                            for ext in [".csv", ".parquet", ".json", ".gz"]
                        )
                    ]
                    original_export_count = sum(
                        1
                        for f in original_analysis.get("staging_patterns", [])
                        if any(
                            ext in f.lower()
                            for ext in ["data file", "export", "compressed"]
                        )
                    )

                    if len(new_export_files) >= original_export_count:
                        self.logger.info(
                            f"✅ Export data files verified: {len(new_export_files)}"
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
        self.logger.info(f"Starting ClickHouse Export Migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info("⚠️  WARNING: This affects active daily staging pipeline!")
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
        success, files_migrated, analysis = self.migrate_clickhouse_export()

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

        if analysis and analysis.get("export_patterns", {}).get("date_patterns"):
            date_patterns = analysis["export_patterns"]["date_patterns"]
            self.logger.info(f"Export operations: {len(date_patterns)} dates detected")

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        else:
            self.logger.info(
                "⚠️  IMPORTANT: Update code references to use new bucket path!"
            )
            self.logger.info(
                "   Update orchestration/dags/jobs/export/export_clickhouse.py"
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
        description="Migrate ClickHouse export data to dedicated export bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  WARNING: This affects active daily staging pipeline!

This migration impacts:
- orchestration/dags/jobs/export/export_clickhouse.py (daily export staging)
- BigQuery → GCS → ClickHouse data pipeline
- External system integration complexity

Coordinate with development team and ClickHouse integration.
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
            f"⚠️  WARNING: This will migrate ClickHouse export data to de-bigquery-data-export-{args.env}"
        )
        print(f"   This affects daily staging pipeline!")
        print(f"   Source: gs://data-bucket-{args.env}/clickhouse_export/")
        print(f"   Target: gs://de-bigquery-data-export-{args.env}/clickhouse_export/")
        print()
        print("After migration, you MUST update code references:")
        print("   - orchestration/dags/jobs/export/export_clickhouse.py")
        print()
        response = input(
            f"Are you sure you want to migrate ClickHouse export data for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = ClickHouseExportMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
