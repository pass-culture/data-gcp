"""File pattern migration class"""

from typing import Dict, List
import re
import sys
import os

# Add the parent directory to the path so we can import base_pattern
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from patterns.base_pattern import BaseMigrationPattern
from utils.logging_utils import log_file_operation


class FilePatternPattern(BaseMigrationPattern):
    """Migration pattern for files matching specific patterns"""

    def discover_files(self) -> List[Dict]:
        """
        Discover files matching specified patterns

        Returns:
            List of file info dictionaries
        """
        source_bucket = self.get_source_bucket()
        source_path = self.get_source_path()
        file_patterns = self.config.get("file_patterns", ["*"])
        include_subfolders = self.config.get("include_subfolders", True)
        exclude_patterns = self.config.get("exclude_patterns", [])

        self.logger.info(
            f"Discovering files with patterns {file_patterns} in gs://{source_bucket}/{source_path}"
        )
        self.logger.info(f"Include subfolders: {include_subfolders}")
        if exclude_patterns:
            self.logger.info(f"Exclude patterns: {exclude_patterns}")

        all_files = []

        # For each pattern, discover matching files
        for pattern in file_patterns:
            self.logger.debug(f"Processing pattern: {pattern}")

            # Convert glob pattern to file types for GCS client
            if pattern.endswith("*"):
                # Handle patterns like "*.csv", "data_*", etc.
                file_types = [pattern]
            else:
                # Specific file names
                file_types = [pattern]

            files = self.gcs_client.list_files(
                bucket=source_bucket,
                prefix=source_path,
                recursive=include_subfolders,
                file_types=file_types,
            )

            # Apply additional filtering based on regex patterns
            filtered_files = self._filter_files_by_pattern(
                files, pattern, exclude_patterns
            )
            all_files.extend(filtered_files)

            self.logger.info(f"Pattern '{pattern}' matched {len(filtered_files)} files")

        # Remove duplicates (in case patterns overlap)
        unique_files = []
        seen_paths = set()
        for file_info in all_files:
            if file_info["name"] not in seen_paths:
                unique_files.append(file_info)
                seen_paths.add(file_info["name"])

        self.logger.info(f"Total unique files discovered: {len(unique_files)}")
        return unique_files

    def _filter_files_by_pattern(
        self, files: List[Dict], pattern: str, exclude_patterns: List[str]
    ) -> List[Dict]:
        """
        Filter files based on regex patterns and exclusions

        Args:
            files: List of file info dictionaries
            pattern: Include pattern (can be glob or regex)
            exclude_patterns: List of exclude patterns

        Returns:
            Filtered list of file info dictionaries
        """
        if not files:
            return []

        filtered_files = []

        # Convert glob pattern to regex if needed
        if "*" in pattern or "?" in pattern:
            # Convert glob to regex
            regex_pattern = pattern.replace("*", ".*").replace("?", ".")
            if not pattern.startswith("^"):
                regex_pattern = f".*{regex_pattern}"
            if not pattern.endswith("$"):
                regex_pattern = f"{regex_pattern}.*"
        else:
            # Treat as exact match or substring
            regex_pattern = f".*{re.escape(pattern)}.*"

        # Compile include pattern
        try:
            include_regex = re.compile(regex_pattern, re.IGNORECASE)
        except re.error as e:
            self.logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            return files  # Return all files if pattern is invalid

        # Compile exclude patterns
        exclude_regexes = []
        for exclude_pattern in exclude_patterns:
            try:
                if "*" in exclude_pattern or "?" in exclude_pattern:
                    exclude_regex = exclude_pattern.replace("*", ".*").replace("?", ".")
                    exclude_regex = f".*{exclude_regex}.*"
                else:
                    exclude_regex = f".*{re.escape(exclude_pattern)}.*"
                exclude_regexes.append(re.compile(exclude_regex, re.IGNORECASE))
            except re.error as e:
                self.logger.warning(
                    f"Invalid exclude regex pattern '{exclude_pattern}': {e}"
                )

        for file_info in files:
            file_name = file_info["name"]

            # Check if file matches include pattern
            if include_regex.search(file_name):
                # Check if file matches any exclude pattern
                excluded = False
                for exclude_regex in exclude_regexes:
                    if exclude_regex.search(file_name):
                        excluded = True
                        self.logger.debug(f"File {file_name} excluded by pattern")
                        break

                if not excluded:
                    filtered_files.append(file_info)

        return filtered_files

    def calculate_target_path(self, source_file_path: str) -> str:
        """
        Calculate target path for a source file with optional renaming

        Args:
            source_file_path: Source file path

        Returns:
            Target file path
        """
        source_path = self.get_source_path()
        target_path = self.get_target_path()
        rename_pattern = self.config.get("rename_pattern")

        # Remove source path prefix
        if source_file_path.startswith(source_path):
            relative_path = source_file_path[len(source_path) :].lstrip("/")
        else:
            relative_path = source_file_path

        # Apply renaming if configured
        if rename_pattern:
            relative_path = self._apply_rename_pattern(relative_path, rename_pattern)

        # Combine with target path
        if target_path:
            return f"{target_path.rstrip('/')}/{relative_path}"
        else:
            return relative_path

    def _apply_rename_pattern(self, file_path: str, rename_pattern: Dict) -> str:
        """
        Apply rename pattern to file path

        Args:
            file_path: Original file path
            rename_pattern: Dictionary with 'from' and 'to' patterns

        Returns:
            Renamed file path
        """
        from_pattern = rename_pattern.get("from", "")
        to_pattern = rename_pattern.get("to", "")

        if not from_pattern:
            return file_path

        try:
            # Apply regex substitution
            renamed_path = re.sub(from_pattern, to_pattern, file_path)

            if renamed_path != file_path:
                self.logger.debug(f"Renamed: {file_path} -> {renamed_path}")

            return renamed_path

        except re.error as e:
            self.logger.warning(f"Invalid rename pattern '{from_pattern}': {e}")
            return file_path

    def migrate_batch(self, files: List[Dict]) -> bool:
        """
        Migrate a batch of files with pattern-based processing

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
            target_file_path = self.calculate_target_path(source_file_path)

            self.logger.debug(f"Migrating: {source_file_path} -> {target_file_path}")

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
        Enhanced dry run with pattern analysis

        Returns:
            Dictionary with dry run results including pattern analysis
        """
        results = super().dry_run()

        if "error" in results:
            return results

        # Add pattern analysis
        file_patterns = self.config.get("file_patterns", ["*"])
        exclude_patterns = self.config.get("exclude_patterns", [])

        try:
            files_to_migrate = self.discover_files()

            # Analyze matches per pattern
            pattern_stats = {}
            for pattern in file_patterns:
                pattern_files = []
                for file_info in files_to_migrate:
                    # Check if this file would match this specific pattern
                    if self._file_matches_pattern(file_info["name"], pattern):
                        pattern_files.append(file_info)

                pattern_stats[pattern] = {
                    "file_count": len(pattern_files),
                    "total_size": sum(f.get("size", 0) for f in pattern_files),
                    "sample_files": [f["name"] for f in pattern_files[:3]],
                }

            results["pattern_analysis"] = pattern_stats
            results["exclude_patterns"] = exclude_patterns

            self.logger.info("Pattern analysis:")
            for pattern, stats in pattern_stats.items():
                size_mb = stats["total_size"] / (1024 * 1024)
                self.logger.info(
                    f"  Pattern '{pattern}': {stats['file_count']} files, {size_mb:.2f} MB"
                )
                if stats["sample_files"]:
                    self.logger.info(f"    Sample files: {stats['sample_files']}")

            if exclude_patterns:
                self.logger.info(f"Exclude patterns: {exclude_patterns}")

        except Exception as e:
            self.logger.error(f"Error during pattern analysis: {e}")

        return results

    def _file_matches_pattern(self, file_path: str, pattern: str) -> bool:
        """
        Check if a file matches a specific pattern

        Args:
            file_path: File path to check
            pattern: Pattern to match against

        Returns:
            True if file matches pattern, False otherwise
        """
        try:
            if "*" in pattern or "?" in pattern:
                regex_pattern = pattern.replace("*", ".*").replace("?", ".")
                regex_pattern = f".*{regex_pattern}.*"
            else:
                regex_pattern = f".*{re.escape(pattern)}.*"

            return bool(re.search(regex_pattern, file_path, re.IGNORECASE))

        except re.error:
            return False
