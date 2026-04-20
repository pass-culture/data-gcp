"""Logging configuration utilities."""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


@dataclass
class DailyStats:
    """Statistics for a single day's processing."""

    date: str
    base: str
    api_results_count: int = 0  # nbreponses from API
    gencods_collected: int = 0  # Unique gencods extracted from search
    filtered_by_date: int = 0  # Gencods filtered by datemodification
    eans_processed: int = 0  # EANs successfully fetched via /ean
    eans_deleted: int = 0  # EANs marked deleted_in_titelive
    eans_failed: int = 0  # EANs that failed API call
    rows_inserted: int = 0  # Rows written to BigQuery
    sample_failed_eans: list = field(default_factory=list)  # Sample of failed EANs
    sample_deleted_eans: list = field(default_factory=list)  # Sample of deleted EANs


@dataclass
class RunStats:
    """Aggregated statistics for the entire run."""

    run_start: datetime = field(default_factory=datetime.now)
    run_end: datetime | None = None
    last_sync_dates: dict = field(default_factory=dict)
    window_start: date | None = None
    window_end: date | None = None
    days_processed: int = 0
    daily_stats: list[DailyStats] = field(default_factory=list)

    # Aggregates
    total_api_results: int = 0
    total_gencods_collected: int = 0
    total_filtered_by_date: int = 0  # Total gencods filtered by datemodification
    total_rows_inserted: int = 0
    total_rows_after_dedup: int = 0

    # Pre/post state tracking
    pre_truncate_count: int = 0
    final_status_breakdown: dict = field(default_factory=dict)
    final_images_status_breakdown: dict = field(default_factory=dict)

    # Warnings
    staleness_days: int = 0
    anomalies: list[str] = field(default_factory=list)

    def add_daily_stats(self, stats: DailyStats) -> None:
        """Add daily stats and update aggregates."""
        self.daily_stats.append(stats)
        self.total_api_results += stats.api_results_count
        self.total_gencods_collected += stats.gencods_collected
        self.total_filtered_by_date += stats.filtered_by_date
        self.total_rows_inserted += stats.rows_inserted

    def log_summary(self, logger: logging.Logger) -> None:
        """Log a comprehensive summary of the run."""
        self.run_end = datetime.now()
        duration = (self.run_end - self.run_start).total_seconds()

        logger.info("=" * 70)
        logger.info("RUN SUMMARY")
        logger.info("=" * 70)

        # Staleness warning
        if self.staleness_days > 2:
            logger.warning(
                f"⚠️  DATA STALENESS: Processing data from {self.staleness_days} "
                f"days ago! Last sync dates: {self.last_sync_dates}"
            )

        # Window info
        logger.info(
            f"📅 Date window: {self.window_start} to {self.window_end} "
            f"({self.days_processed} days)"
        )
        logger.info(f"🔄 Last sync dates from provider: {self.last_sync_dates}")

        # Data flow summary with filtering info
        logger.info("-" * 70)
        logger.info("DATA FLOW:")
        logger.info(
            f"  1. API search results (nbreponses):  {self.total_api_results:,}"
        )
        logger.info(
            f"  2. Gencods collected from search:    {self.total_gencods_collected:,}"
        )
        if self.total_filtered_by_date > 0:
            filtered = self.total_filtered_by_date
            logger.info(f"     └─ Filtered by date:              {filtered:,}")
        logger.info(
            f"  3. Rows inserted to BigQuery:        {self.total_rows_inserted:,}"
        )
        logger.info(
            f"  4. Rows after deduplication:         {self.total_rows_after_dedup:,}"
        )
        dedup_removed = self.total_rows_inserted - self.total_rows_after_dedup
        if dedup_removed > 0:
            logger.info(f"     └─ Duplicates removed:            {dedup_removed:,}")

        # Pre-truncate vs post comparison
        if self.pre_truncate_count > 0:
            diff = self.total_rows_after_dedup - self.pre_truncate_count
            diff_pct = (
                (diff / self.pre_truncate_count * 100)
                if self.pre_truncate_count > 0
                else 0
            )
            logger.info("-" * 70)
            logger.info("TABLE COMPARISON (before vs after):")
            logger.info(f"  Before truncate: {self.pre_truncate_count:,}")
            logger.info(f"  After run:       {self.total_rows_after_dedup:,}")
            logger.info(f"  Difference:      {diff:+,} ({diff_pct:+.1f}%)")

        # Per-day breakdown
        logger.info("-" * 70)
        logger.info("PER-DAY BREAKDOWN:")
        logger.info(
            f"{'Date':<12} {'Base':<8} {'API':>8} {'Gencods':>8} "
            f"{'Filtered':>8} {'Inserted':>8} {'Failed':>6}"
        )
        logger.info("-" * 70)
        for stats in self.daily_stats:
            logger.info(
                f"{stats.date:<12} {stats.base:<8} {stats.api_results_count:>8,} "
                f"{stats.gencods_collected:>8,} {stats.filtered_by_date:>8,} "
                f"{stats.rows_inserted:>8,} {stats.eans_failed:>6,}"
            )

        # Anomalies
        if self.anomalies:
            logger.info("-" * 70)
            logger.warning("⚠️  ANOMALIES DETECTED:")
            for anomaly in self.anomalies:
                logger.warning(f"  - {anomaly}")

        # Timing
        logger.info("-" * 70)
        logger.info(f"⏱️  Total duration: {duration:.1f}s ({duration/60:.1f}min)")
        logger.info("=" * 70)
