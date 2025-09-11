"""Logging utilities for migration operations"""

import logging
import os
from datetime import datetime
from typing import Optional


def setup_migration_logging(
    env: str,
    use_case: Optional[str] = None,
    log_level: str = "INFO",
    log_dir: str = "logs",
) -> logging.Logger:
    """
    Setup comprehensive logging for migration operations

    Args:
        env: Environment (dev, stg, prod)
        use_case: Specific use case being migrated (optional)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory to store log files

    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if use_case:
        log_filename = f"migration_{env}_{use_case}_{timestamp}.log"
    else:
        log_filename = f"migration_{env}_{timestamp}.log"

    log_filepath = os.path.join(log_dir, log_filename)

    # Configure logging format
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )

    # Setup file handler
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_handler.setFormatter(formatter)

    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Clear existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Create migration-specific logger
    migration_logger = logging.getLogger("migration")
    migration_logger.info(f"Migration logging started - Log file: {log_filepath}")

    return migration_logger


def log_migration_summary(
    logger: logging.Logger, results: dict, start_time: datetime, end_time: datetime
):
    """
    Log a comprehensive migration summary

    Args:
        logger: Logger instance
        results: Dictionary of migration results per use case
        start_time: Migration start time
        end_time: Migration end time
    """
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Start time: {start_time}")
    logger.info(f"End time: {end_time}")
    logger.info(f"Duration: {duration}")
    logger.info("")

    successful = sum(1 for success in results.values() if success)
    failed = len(results) - successful

    logger.info(f"Total use cases: {len(results)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info("")

    if results:
        logger.info("Results by use case:")
        for use_case, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            logger.info(f"  {use_case}: {status}")

    logger.info("=" * 60)


def log_file_operation(
    logger: logging.Logger,
    operation: str,
    source: str,
    target: str,
    success: bool,
    error: Optional[str] = None,
):
    """
    Log individual file operations with consistent formatting

    Args:
        logger: Logger instance
        operation: Operation type (COPY, DELETE, etc.)
        source: Source file path
        target: Target file path (optional for DELETE)
        success: Whether operation was successful
        error: Error message if operation failed
    """
    if success:
        if target:
            logger.debug(f"[{operation}] ✅ {source} -> {target}")
        else:
            logger.debug(f"[{operation}] ✅ {source}")
    else:
        if target:
            logger.error(f"[{operation}] ❌ {source} -> {target}")
        else:
            logger.error(f"[{operation}] ❌ {source}")

        if error:
            logger.error(f"  Error: {error}")


def log_batch_progress(
    logger: logging.Logger,
    batch_num: int,
    total_batches: int,
    files_in_batch: int,
    total_files: int,
):
    """
    Log batch processing progress

    Args:
        logger: Logger instance
        batch_num: Current batch number (1-based)
        total_batches: Total number of batches
        files_in_batch: Number of files in current batch
        total_files: Total number of files to process
    """
    progress_pct = (batch_num / total_batches) * 100
    files_processed = (
        batch_num * files_in_batch if batch_num < total_batches else total_files
    )

    logger.info(
        f"Processing batch {batch_num}/{total_batches} "
        f"({files_in_batch} files) - "
        f"Progress: {files_processed}/{total_files} files ({progress_pct:.1f}%)"
    )


class MigrationProgressTracker:
    """Track and log migration progress with detailed statistics"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.stats = {
            "files_processed": 0,
            "files_successful": 0,
            "files_failed": 0,
            "bytes_processed": 0,
            "start_time": datetime.now(),
        }

    def log_file_processed(self, file_info: dict, success: bool):
        """Log a processed file and update statistics"""
        self.stats["files_processed"] += 1

        if success:
            self.stats["files_successful"] += 1
            self.stats["bytes_processed"] += file_info.get("size", 0)
        else:
            self.stats["files_failed"] += 1

    def log_progress(self, total_files: int):
        """Log current progress statistics"""
        processed = self.stats["files_processed"]
        progress_pct = (processed / total_files) * 100 if total_files > 0 else 0

        elapsed = datetime.now() - self.stats["start_time"]

        self.logger.info(
            f"Progress: {processed}/{total_files} files ({progress_pct:.1f}%) - "
            f"Success: {self.stats['files_successful']}, "
            f"Failed: {self.stats['files_failed']}, "
            f"Elapsed: {elapsed}"
        )

    def log_final_stats(self):
        """Log final migration statistics"""
        elapsed = datetime.now() - self.stats["start_time"]

        self.logger.info("Final Statistics:")
        self.logger.info(f"  Files processed: {self.stats['files_processed']}")
        self.logger.info(f"  Files successful: {self.stats['files_successful']}")
        self.logger.info(f"  Files failed: {self.stats['files_failed']}")
        self.logger.info(f"  Bytes processed: {self.stats['bytes_processed']:,}")
        self.logger.info(f"  Total duration: {elapsed}")

        if self.stats["files_processed"] > 0:
            success_rate = (
                self.stats["files_successful"] / self.stats["files_processed"]
            ) * 100
            self.logger.info(f"  Success rate: {success_rate:.1f}%")

            # Calculate throughput
            elapsed_seconds = elapsed.total_seconds()
            if elapsed_seconds > 0:
                files_per_second = self.stats["files_processed"] / elapsed_seconds
                mb_per_second = (
                    self.stats["bytes_processed"] / (1024 * 1024)
                ) / elapsed_seconds
                self.logger.info(
                    f"  Throughput: {files_per_second:.2f} files/sec, "
                    f"{mb_per_second:.2f} MB/sec"
                )
