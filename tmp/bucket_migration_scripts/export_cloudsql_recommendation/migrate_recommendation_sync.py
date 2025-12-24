#!/usr/bin/env python3
"""
CloudSQL Recommendation Sync Migration Script

This script migrates CloudSQL recommendation sync data from the old bucket structure
to the new dedicated export bucket structure.

Migration path:
- From: gs://data-bucket-{env}/export/cloudsql_recommendation_tables_to_bigquery/
- To: gs://de-bigquery-data-export-{env}/export_cloudsql_recommendation/

Usage:
    python migrate_recommendation_sync.py --env prod --dry-run
    python migrate_recommendation_sync.py --env prod --execute
"""

import argparse
import logging
import re
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Tuple


class RecommendationSyncMigrator:
    """Handles migration of CloudSQL recommendation sync data from old to new bucket structure."""

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
        log_filename = f"migrate_recommendation_sync_{env}_{timestamp}.log"

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

    def list_recommendation_sync_contents(self) -> List[str]:
        """List all contents in the recommendation sync folder."""
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
                        "Recommendation sync folder does not exist or is empty"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing recommendation sync contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning recommendation sync folder: {e}")
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

    def analyze_recommendation_sync_files(self, files: List[str]) -> Dict[str, any]:
        """Analyze the recommendation sync files and provide insights."""
        analysis = {
            "total_files": len(files),
            "file_types": {},
            "timestamp_patterns": {"timestamps": set(), "recent_syncs": []},
            "sync_patterns": [],
        }

        # Pattern to extract timestamps from sync folders/files
        timestamp_pattern = re.compile(r"(\d{8}[_T]\d{6}|\d{14}|\d{10})")

        for file_path in files:
            filename = file_path.split("/")[-1]
            extension = filename.split(".")[-1].lower() if "." in filename else "no_ext"

            # Count file types
            if extension in analysis["file_types"]:
                analysis["file_types"][extension] += 1
            else:
                analysis["file_types"][extension] = 1

            # Extract timestamp patterns from path
            timestamp_match = timestamp_pattern.search(file_path)
            if timestamp_match:
                timestamp_str = timestamp_match.group(1)
                analysis["timestamp_patterns"]["timestamps"].add(timestamp_str)

            # Identify sync patterns
            if "recommendation" in filename.lower():
                if filename.endswith((".csv", ".parquet", ".json")):
                    analysis["sync_patterns"].append(f"📊 {filename} (Sync data file)")
                else:
                    analysis["sync_patterns"].append(f"📄 {filename} (Sync file)")
            elif filename.endswith((".csv", ".parquet", ".json")):
                analysis["sync_patterns"].append(f"📊 {filename} (Data file)")
            else:
                analysis["sync_patterns"].append(f"📄 {filename}")

        # Convert timestamps set to sorted list and identify recent ones
        all_timestamps = sorted(list(analysis["timestamp_patterns"]["timestamps"]))
        analysis["timestamp_patterns"]["timestamps"] = all_timestamps

        # Get recent timestamps (assuming they represent sync operations)
        if all_timestamps:
            analysis["timestamp_patterns"]["recent_syncs"] = all_timestamps[
                -5:
            ]  # Last 5

        return analysis

    def migrate_recommendation_sync(self) -> Tuple[bool, int, Dict]:
        """
        Migrate all recommendation sync files.

        Returns:
            Tuple of (success, files_count, analysis)
        """
        self.logger.info("Starting CloudSQL recommendation sync migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_recommendation_sync_contents()
        if not files:
            self.logger.info("No recommendation sync files to migrate")
            return True, 0, {}

        # Analyze files
        analysis = self.analyze_recommendation_sync_files(files)
        self.logger.info(f"Found {len(files)} recommendation sync files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        # Log analysis
        self.logger.info("File analysis:")
        self.logger.info(f"  Total files: {analysis['total_files']}")
        self.logger.info(f"  File types: {analysis['file_types']}")

        # Show sync operation patterns
        timestamps = analysis["timestamp_patterns"]["timestamps"]
        recent_syncs = analysis["timestamp_patterns"]["recent_syncs"]

        if timestamps:
            self.logger.info(f"  Sync operations detected: {len(timestamps)}")
            if recent_syncs:
                self.logger.info(f"  Recent sync timestamps: {recent_syncs}")

        if analysis["sync_patterns"]:
            self.logger.info("  Sample sync files:")
            for pattern in analysis["sync_patterns"][:5]:  # Show first 5
                self.logger.info(f"    {pattern}")

        if self.dry_run:
            self.logger.info(
                "DRY RUN - Recommendation sync files that would be migrated:"
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
                self.logger.info("Successfully migrated recommendation sync files")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files), analysis
            else:
                self.logger.error("Recommendation sync migration failed")
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

                    # Verify sync data files
                    new_data_files = [
                        f
                        for f in new_files
                        if any(
                            ext in f.lower() for ext in [".csv", ".parquet", ".json"]
                        )
                    ]
                    original_data_count = sum(
                        1
                        for f in original_analysis.get("sync_patterns", [])
                        if "data file" in f.lower()
                    )

                    if len(new_data_files) >= original_data_count:
                        self.logger.info(
                            f"✅ Sync data files verified: {len(new_data_files)}"
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
        self.logger.info("Starting CloudSQL Recommendation Sync Migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info(
            "⚠️  WARNING: This affects active bidirectional sync operations!"
        )
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
        success, files_migrated, analysis = self.migrate_recommendation_sync()

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

        if analysis and analysis.get("timestamp_patterns", {}).get("timestamps"):
            timestamps = analysis["timestamp_patterns"]["timestamps"]
            self.logger.info(f"Sync operations: {len(timestamps)} detected")

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        else:
            self.logger.info(
                "⚠️  IMPORTANT: Update code references to use new bucket path!"
            )
            self.logger.info(
                "   Update orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py"
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
        description="Migrate CloudSQL recommendation sync data to dedicated export bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  WARNING: This affects active bidirectional sync operations!

This migration impacts:
- orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py
- Bidirectional sync operations between BigQuery and CloudSQL
- Recommendation sync staging and coordination

Coordinate with development team for complex sync operations.
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
            f"⚠️  WARNING: This will migrate CloudSQL recommendation sync data to de-bigquery-data-export-{args.env}"
        )
        print("   This affects bidirectional sync operations!")
        print(
            f"   Source: gs://data-bucket-{args.env}/export/cloudsql_recommendation_tables_to_bigquery/"
        )
        print(
            f"   Target: gs://de-bigquery-data-export-{args.env}/export_cloudsql_recommendation/"
        )
        print()
        print("After migration, you MUST update code references:")
        print(
            "   - orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py"
        )
        print()
        response = input(
            f"Are you sure you want to migrate recommendation sync data for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = RecommendationSyncMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
