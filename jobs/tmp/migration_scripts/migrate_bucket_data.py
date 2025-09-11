#!/usr/bin/env python3
"""
Main migration orchestrator script

This script orchestrates bucket data migrations using different patterns.
It can handle various migration scenarios through configuration-driven approach.

Usage:
    python migrate_bucket_data.py --config config/recommendation_sync.yaml --env dev --dry-run
    python migrate_bucket_data.py --config config/seed_data.yaml --env prod
    python migrate_bucket_data.py --use-case recommendation_sync --env stg
"""

import argparse
import yaml
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logging_utils import setup_migration_logging, log_migration_summary
from patterns.base_pattern import BaseMigrationPattern
from patterns.single_folder import SingleFolderPattern
from patterns.multi_folder import MultiFolderPattern
from patterns.file_pattern import FilePatternPattern
from patterns.date_partitioned import DatePartitionedPattern


class MigrationOrchestrator:
    """Main orchestrator for bucket data migrations"""

    PATTERN_CLASSES = {
        "single_folder": SingleFolderPattern,
        "multi_folder": MultiFolderPattern,
        "file_pattern": FilePatternPattern,
        "date_partitioned": DatePartitionedPattern,
    }

    def __init__(self, config_path: str = None, use_case: str = None, env: str = "dev"):
        """
        Initialize migration orchestrator

        Args:
            config_path: Path to configuration file
            use_case: Predefined use case name
            env: Environment (dev, stg, prod)
        """
        self.env = env
        self.config = None
        self.logger = None

        if config_path:
            self.config = self._load_config(config_path)
        elif use_case:
            self.config = self._load_use_case_config(use_case)
        else:
            raise ValueError("Either config_path or use_case must be provided")

        # Setup logging
        self.logger = setup_migration_logging(
            env=env,
            use_case=use_case or self._extract_use_case_from_config(),
            log_level=self.config.get("log_level", "INFO"),
        )

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            self.logger.info(f"Loaded configuration from {config_path}")
            return config
        except Exception as e:
            print(f"Error loading configuration from {config_path}: {e}")
            sys.exit(1)

    def _load_use_case_config(self, use_case: str) -> Dict:
        """Load predefined use case configuration"""
        config_file = f"config/{use_case}.yaml"

        if not os.path.exists(config_file):
            print(
                f"Configuration file for use case '{use_case}' not found: {config_file}"
            )
            sys.exit(1)

        return self._load_config(config_file)

    def _extract_use_case_from_config(self) -> str:
        """Extract use case name from configuration"""
        return self.config.get("use_case", "unknown")

    def _create_migration_pattern(self, pattern_config: Dict) -> BaseMigrationPattern:
        """
        Create migration pattern instance based on configuration

        Args:
            pattern_config: Pattern configuration dictionary

        Returns:
            Migration pattern instance
        """
        pattern_type = pattern_config["pattern"]

        if pattern_type not in self.PATTERN_CLASSES:
            raise ValueError(f"Unknown pattern type: {pattern_type}")

        pattern_class = self.PATTERN_CLASSES[pattern_type]
        return pattern_class(pattern_config, self.env)

    def run_migration(self, dry_run: bool = False) -> bool:
        """
        Run migration for all configured patterns

        Args:
            dry_run: If True, only analyze what would be migrated

        Returns:
            True if all migrations successful, False otherwise
        """
        start_time = datetime.now()

        self.logger.info("=" * 60)
        self.logger.info("BUCKET MIGRATION ORCHESTRATOR")
        self.logger.info("=" * 60)
        self.logger.info(f"Environment: {self.env}")
        self.logger.info(f"Dry run: {dry_run}")
        self.logger.info(f"Start time: {start_time}")

        # Get migration patterns from configuration
        patterns = self.config.get("patterns", [])
        if not patterns:
            # Single pattern configuration (backward compatibility)
            patterns = [self.config]

        results = {}

        for i, pattern_config in enumerate(patterns, 1):
            pattern_name = pattern_config.get("name", f"pattern_{i}")
            self.logger.info(
                f"\nProcessing pattern {i}/{len(patterns)}: {pattern_name}"
            )

            try:
                # Create pattern instance
                migration_pattern = self._create_migration_pattern(pattern_config)

                if dry_run:
                    # Run dry run analysis
                    self.logger.info(f"Running dry run for {pattern_name}...")
                    dry_run_results = migration_pattern.dry_run()

                    if "error" in dry_run_results:
                        self.logger.error(
                            f"Dry run failed for {pattern_name}: {dry_run_results['error']}"
                        )
                        results[pattern_name] = False
                    else:
                        self._log_dry_run_results(pattern_name, dry_run_results)
                        results[pattern_name] = True
                else:
                    # Run actual migration
                    self.logger.info(f"Running migration for {pattern_name}...")
                    success = migration_pattern.execute()
                    results[pattern_name] = success

                    if success:
                        self.logger.info(
                            f"✅ Migration completed successfully for {pattern_name}"
                        )
                    else:
                        self.logger.error(f"❌ Migration failed for {pattern_name}")

            except Exception as e:
                self.logger.error(f"Error processing pattern {pattern_name}: {e}")
                results[pattern_name] = False

        # Log final summary
        end_time = datetime.now()
        log_migration_summary(self.logger, results, start_time, end_time)

        # Return overall success status
        return all(results.values())

    def _log_dry_run_results(self, pattern_name: str, results: Dict):
        """Log dry run results in a readable format"""
        self.logger.info(f"Dry run results for {pattern_name}:")
        self.logger.info(f"  Source bucket: {results.get('source_bucket', 'N/A')}")
        self.logger.info(f"  Target bucket: {results.get('target_bucket', 'N/A')}")
        self.logger.info(f"  Files to migrate: {results.get('total_files', 0)}")
        self.logger.info(f"  Total size: {results.get('total_size_mb', 0):.2f} MB")

        # Log file types breakdown
        file_types = results.get("file_types", {})
        if file_types:
            self.logger.info("  File types breakdown:")
            for ext, count in sorted(file_types.items()):
                self.logger.info(f"    {ext}: {count} files")

        # Log pattern-specific analysis
        if "subfolder_analysis" in results:
            self.logger.info("  Subfolder analysis:")
            for subfolder, stats in results["subfolder_analysis"].items():
                size_mb = stats["total_size"] / (1024 * 1024)
                self.logger.info(
                    f"    {subfolder}: {stats['file_count']} files, {size_mb:.2f} MB"
                )

        if "pattern_analysis" in results:
            self.logger.info("  Pattern analysis:")
            for pattern, stats in results["pattern_analysis"].items():
                size_mb = stats["total_size"] / (1024 * 1024)
                self.logger.info(
                    f"    '{pattern}': {stats['file_count']} files, {size_mb:.2f} MB"
                )

        if "date_analysis" in results:
            self.logger.info("  Date partition analysis:")
            for date, stats in sorted(results["date_analysis"].items()):
                size_mb = stats["total_size"] / (1024 * 1024)
                self.logger.info(
                    f"    {date}: {stats['file_count']} files, {size_mb:.2f} MB"
                )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Bucket Data Migration Orchestrator")

    # Configuration options (mutually exclusive)
    config_group = parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument(
        "--config", "-c", help="Path to migration configuration file"
    )
    config_group.add_argument(
        "--use-case",
        "-u",
        choices=[
            "recommendation_sync",
            "seed_data",
            "clickhouse_export",
            "dms_export",
            "historization",
        ],
        help="Predefined use case configuration",
    )

    parser.add_argument(
        "--env",
        "-e",
        choices=["dev", "stg", "prod"],
        default="dev",
        help="Environment to run migration for (default: dev)",
    )

    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Run dry run to analyze what would be migrated",
    )

    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    try:
        # Create orchestrator
        orchestrator = MigrationOrchestrator(
            config_path=args.config, use_case=args.use_case, env=args.env
        )

        # Override log level if specified
        if hasattr(args, "log_level"):
            orchestrator.config["log_level"] = args.log_level

        # Run migration
        success = orchestrator.run_migration(dry_run=args.dry_run)

        # Exit with appropriate code
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration orchestrator error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
