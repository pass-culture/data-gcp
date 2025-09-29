#!/usr/bin/env python3
"""
Elementary Reports Migration Script

This script migrates Elementary report data from the old bucket structure to the new
dedicated export bucket structure.

Migration path:
- From: gs://data-bucket-{env}/elementary_reports/
- To: gs://de-bigquery-data-export-{env}/elementary_reports/

Usage:
    python migrate_elementary_reports.py --env prod --dry-run
    python migrate_elementary_reports.py --env prod --execute
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import re


class ElementaryReportsMigrator:
    """Handles migration of Elementary reports from old to new bucket structure."""

    def __init__(self, env: str, dry_run: bool = True):
        self.env = env
        self.dry_run = dry_run
        self.old_bucket = f"data-bucket-{env}"
        self.new_bucket = f"de-bigquery-data-export-{env}"
        self.old_path = f"gs://{self.old_bucket}/elementary_reports/"
        self.new_path = f"gs://{self.new_bucket}/elementary_reports/"

        # Configure logging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"migrate_elementary_reports_{env}_{timestamp}.log"

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

    def list_elementary_reports_contents(self) -> List[str]:
        """List all contents in the elementary_reports folder."""
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
                        "elementary_reports folder does not exist or is empty"
                    )
                    return []
                else:
                    self.logger.error(
                        f"Error listing elementary_reports contents: {result.stderr}"
                    )
                    return []

            # Filter out directories, keep only files
            files = []
            for line in result.stdout.strip().split("\n"):
                if line and not line.endswith("/"):  # Skip directories
                    files.append(line)

            return files

        except subprocess.SubprocessError as e:
            self.logger.error(f"Error scanning elementary_reports folder: {e}")
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

    def analyze_elementary_reports(self, files: List[str]) -> Dict[str, any]:
        """Analyze the Elementary report files and provide insights."""
        analysis = {
            "total_files": len(files),
            "file_types": {},
            "date_organization": {"years": set(), "months": set()},
            "report_patterns": [],
        }

        # Pattern to extract dates from report names: elementary_report_YYYYMMDD.html
        date_pattern = re.compile(r"elementary_report_(\d{8})\.html")

        for file_path in files:
            filename = file_path.split("/")[-1]
            extension = filename.split(".")[-1].lower() if "." in filename else "no_ext"

            # Count file types
            if extension in analysis["file_types"]:
                analysis["file_types"][extension] += 1
            else:
                analysis["file_types"][extension] = 1

            # Extract date organization patterns
            path_parts = file_path.replace(self.old_path, "").split("/")
            if (
                len(path_parts) > 0
                and path_parts[0].isdigit()
                and len(path_parts[0]) == 4
            ):
                # Year-based organization: elementary_reports/2023/elementary_report_20231215.html
                year = path_parts[0]
                analysis["date_organization"]["years"].add(year)

                # Extract month from filename if possible
                date_match = date_pattern.search(filename)
                if date_match:
                    date_str = date_match.group(1)  # YYYYMMDD
                    month = date_str[:6]  # YYYYMM
                    analysis["date_organization"]["months"].add(month)

            # Identify report patterns
            if filename.startswith("elementary_report_") and filename.endswith(".html"):
                analysis["report_patterns"].append(
                    f"📊 {filename} (Daily monitoring report)"
                )
            elif filename.endswith(".html"):
                analysis["report_patterns"].append(f"📄 {filename} (HTML report)")
            else:
                analysis["report_patterns"].append(f"📄 {filename}")

        # Convert sets to sorted lists for display
        analysis["date_organization"]["years"] = sorted(
            list(analysis["date_organization"]["years"])
        )
        analysis["date_organization"]["months"] = sorted(
            list(analysis["date_organization"]["months"])
        )

        return analysis

    def migrate_elementary_reports(self) -> Tuple[bool, int, Dict]:
        """
        Migrate all Elementary report files.

        Returns:
            Tuple of (success, files_count, analysis)
        """
        self.logger.info(f"Starting Elementary reports migration")
        self.logger.info(f"  From: {self.old_path}")
        self.logger.info(f"  To: {self.new_path}")

        # Check if source exists
        files = self.list_elementary_reports_contents()
        if not files:
            self.logger.info("No Elementary report files to migrate")
            return True, 0, {}

        # Analyze files
        analysis = self.analyze_elementary_reports(files)
        self.logger.info(f"Found {len(files)} Elementary report files to migrate")

        # Get total size
        total_size = self.get_folder_size(self.old_path)
        self.logger.info(f"Total size: {total_size}")

        # Log analysis
        self.logger.info("File analysis:")
        self.logger.info(f"  Total files: {analysis['total_files']}")
        self.logger.info(f"  File types: {analysis['file_types']}")

        # Show date organization
        if analysis["date_organization"]["years"]:
            self.logger.info(
                f"  Years with reports: {analysis['date_organization']['years']}"
            )

        if analysis["date_organization"]["months"]:
            recent_months = analysis["date_organization"]["months"][
                -6:
            ]  # Show last 6 months
            self.logger.info(f"  Recent months: {recent_months}")

        if analysis["report_patterns"]:
            self.logger.info("  Sample reports:")
            for pattern in analysis["report_patterns"][:5]:  # Show first 5
                self.logger.info(f"    {pattern}")

        if self.dry_run:
            self.logger.info(
                "DRY RUN - Elementary report files that would be migrated:"
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
                self.logger.info("Successfully migrated Elementary report files")
                if result.stdout:
                    self.logger.info(f"gcloud storage output: {result.stdout}")
                return True, len(files), analysis
            else:
                self.logger.error("Elementary reports migration failed")
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

                    # Verify HTML report files
                    new_html_files = [f for f in new_files if f.endswith(".html")]
                    original_html_count = original_analysis.get("file_types", {}).get(
                        "html", 0
                    )

                    if len(new_html_files) >= original_html_count:
                        self.logger.info(
                            f"✅ HTML report files verified: {len(new_html_files)}"
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
        self.logger.info(f"Starting Elementary Reports Migration")
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'EXECUTION'}")
        self.logger.info(
            "⚠️  NOTE: This affects daily report generation and growing volume!"
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
        success, files_migrated, analysis = self.migrate_elementary_reports()

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

        if analysis and analysis.get("date_organization", {}).get("years"):
            years = analysis["date_organization"]["years"]
            self.logger.info(
                f"Report coverage: {len(years)} years ({years[0]} - {years[-1]})"
            )

        if self.dry_run:
            self.logger.info("DRY RUN - No actual migration performed")
        else:
            self.logger.info(
                "⚠️  IMPORTANT: Update code references to use new bucket path!"
            )
            self.logger.info("   Update orchestration/dags/jobs/dbt/dbt_artifacts.py")
            self.logger.info(
                f"   Change path from: gs://data-bucket-{self.env}/elementary_reports/"
            )
            self.logger.info(
                f"   To: gs://de-bigquery-data-export-{self.env}/elementary_reports/"
            )
            self.logger.info(
                "   Consider implementing retention policy for growing reports"
            )

        self.logger.info("=" * 80)

        return success


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Elementary reports to dedicated export bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  NOTE: This affects daily report generation and growing volume!

This migration impacts:
- orchestration/dags/jobs/dbt/dbt_artifacts.py (daily report creation)
- Manual report viewing and access
- Report archive organization and retention

Consider implementing retention policy after migration.
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
            f"⚠️  WARNING: This will migrate Elementary reports to de-bigquery-data-export-{args.env}"
        )
        print(f"   This affects daily report generation!")
        print(f"   Source: gs://data-bucket-{args.env}/elementary_reports/")
        print(f"   Target: gs://de-bigquery-data-export-{args.env}/elementary_reports/")
        print()
        print("After migration, you MUST update code references:")
        print("   - orchestration/dags/jobs/dbt/dbt_artifacts.py")
        print()
        print("Consider implementing retention policy for growing reports.")
        print()
        response = input(
            f"Are you sure you want to migrate Elementary reports for {args.env}? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return 1

    migrator = ElementaryReportsMigrator(args.env, dry_run)
    success = migrator.run_migration()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
