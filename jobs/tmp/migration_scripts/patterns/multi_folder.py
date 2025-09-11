"""Multi folder migration pattern"""

from typing import Dict, List
import sys
import os

# Add the parent directory to the path so we can import base_pattern
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from patterns.base_pattern import BaseMigrationPattern
from utils.logging_utils import log_file_operation


class MultiFolderPattern(BaseMigrationPattern):
    """Migration pattern for multiple folders with optional mapping"""

    def discover_files(self) -> List[Dict]:
        """
        Discover files in multiple subfolders

        Returns:
            List of file info dictionaries
        """
        source_bucket = self.get_source_bucket()
        source_path = self.get_source_path()
        file_types = self.config.get("file_types", ["*"])
        subfolder_mapping = self.config.get("subfolder_mapping", {})

        self.logger.info(
            f"Discovering files in multiple subfolders under gs://{source_bucket}/{source_path}"
        )
        self.logger.info(f"File types: {file_types}")

        if subfolder_mapping:
            self.logger.info(f"Using subfolder mapping: {subfolder_mapping}")
            # Discover files in specific mapped subfolders
            all_files = []
            for source_subfolder in subfolder_mapping.keys():
                subfolder_path = f"{source_path}{source_subfolder}".rstrip("/")
                self.logger.info(f"Discovering files in subfolder: {subfolder_path}")

                files = self.gcs_client.list_files(
                    bucket=source_bucket,
                    prefix=subfolder_path,
                    recursive=True,
                    file_types=file_types,
                )

                # Add subfolder context to file info
                for file_info in files:
                    file_info["source_subfolder"] = source_subfolder

                all_files.extend(files)
                self.logger.info(f"Found {len(files)} files in {subfolder_path}")
        else:
            # Discover all files recursively
            all_files = self.gcs_client.list_files(
                bucket=source_bucket,
                prefix=source_path,
                recursive=True,
                file_types=file_types,
            )

        self.logger.info(f"Total discovered files: {len(all_files)}")
        return all_files

    def calculate_target_path(
        self, source_file_path: str, source_subfolder: str = None
    ) -> str:
        """
        Calculate target path for a source file, applying subfolder mapping if configured

        Args:
            source_file_path: Source file path
            source_subfolder: Source subfolder (if using mapping)

        Returns:
            Target file path
        """
        source_path = self.get_source_path()
        target_path = self.get_target_path()
        subfolder_mapping = self.config.get("subfolder_mapping", {})

        # Remove source path prefix
        if source_file_path.startswith(source_path):
            relative_path = source_file_path[len(source_path) :].lstrip("/")
        else:
            relative_path = source_file_path

        # Apply subfolder mapping if configured
        if subfolder_mapping and source_subfolder:
            target_subfolder = subfolder_mapping.get(source_subfolder, source_subfolder)

            # Replace source subfolder with target subfolder in relative path
            if relative_path.startswith(source_subfolder):
                relative_path = relative_path[len(source_subfolder) :].lstrip("/")
                relative_path = f"{target_subfolder.rstrip('/')}/{relative_path}"

        # Combine with target path
        if target_path:
            return f"{target_path.rstrip('/')}/{relative_path}"
        else:
            return relative_path

    def migrate_batch(self, files: List[Dict]) -> bool:
        """
        Migrate a batch of files with subfolder mapping

        Args:
            files: List of file info dictionaries to migrate

        Returns:
            True if batch migration successful, False otherwise
        """
        source_bucket = self.get_source_bucket()
        target_bucket = self.get_target_bucket()

        self.logger.debug(f"Migrating batch of {len(files)} files")
        self.logger.debug(f"Source bucket: {source_bucket}")
        self.logger.debug(f"Target bucket: {target_bucket}")

        for file_info in files:
            source_file_path = file_info["name"]
            source_subfolder = file_info.get("source_subfolder")

            target_file_path = self.calculate_target_path(
                source_file_path, source_subfolder
            )

            self.logger.debug(f"Migrating: {source_file_path} -> {target_file_path}")
            if source_subfolder:
                self.logger.debug(f"  Subfolder mapping: {source_subfolder}")

            # Copy the file
            success = self.gcs_client.copy_file(
                source_bucket=source_bucket,
                source_path=source_file_path,
                target_bucket=target_bucket,
                target_path=target_file_path,
            )

            # Log the operation
            log_file_operation(
                self.logger,
                "COPY",
                f"gs://{source_bucket}/{source_file_path}",
                f"gs://{target_bucket}/{target_file_path}",
                success,
            )

            if not success:
                self.logger.error(f"Failed to copy {source_file_path}")
                return False

            # Track progress
            self.progress_tracker.log_file_processed(file_info, success)

            # Store target path in file info for validation
            file_info["target_path"] = target_file_path

        return True

    def dry_run(self) -> Dict:
        """
        Enhanced dry run with subfolder analysis

        Returns:
            Dictionary with dry run results including subfolder breakdown
        """
        results = super().dry_run()

        if "error" in results:
            return results

        # Add subfolder analysis
        subfolder_mapping = self.config.get("subfolder_mapping", {})
        if subfolder_mapping:
            subfolder_stats = {}

            try:
                files_to_migrate = self.discover_files()

                for file_info in files_to_migrate:
                    source_subfolder = file_info.get("source_subfolder", "unknown")
                    target_subfolder = subfolder_mapping.get(
                        source_subfolder, source_subfolder
                    )

                    if target_subfolder not in subfolder_stats:
                        subfolder_stats[target_subfolder] = {
                            "file_count": 0,
                            "total_size": 0,
                            "source_subfolders": set(),
                        }

                    subfolder_stats[target_subfolder]["file_count"] += 1
                    subfolder_stats[target_subfolder]["total_size"] += file_info.get(
                        "size", 0
                    )
                    subfolder_stats[target_subfolder]["source_subfolders"].add(
                        source_subfolder
                    )

                # Convert sets to lists for JSON serialization
                for stats in subfolder_stats.values():
                    stats["source_subfolders"] = list(stats["source_subfolders"])

                results["subfolder_analysis"] = subfolder_stats

                self.logger.info("Subfolder analysis:")
                for target_subfolder, stats in subfolder_stats.items():
                    size_mb = stats["total_size"] / (1024 * 1024)
                    self.logger.info(
                        f"  {target_subfolder}: {stats['file_count']} files, {size_mb:.2f} MB"
                    )
                    self.logger.info(
                        f"    Source subfolders: {stats['source_subfolders']}"
                    )

            except Exception as e:
                self.logger.error(f"Error during subfolder analysis: {e}")

        return results
