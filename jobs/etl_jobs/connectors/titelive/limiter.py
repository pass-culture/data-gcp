"""Rate limiters for Titelive API."""

import logging
import time

from connectors.titelive.config import (
    BURST_RATE,
    BURST_SIZE,
    RECOVERY_TIME,
    SUSTAINED_RATE,
)
from http_tools.rate_limiters import BaseRateLimiter

logger = logging.getLogger(__name__)


class TiteliveBasicRateLimiter(BaseRateLimiter):
    """
    Basic rate limiter for Phase 1 (factory pattern migration).

    Conservative 1 req/sec to match current behavior.
    This will be replaced by TiteliveBurstRecoveryLimiter in Phase 2B.
    """

    def __init__(self):
        self.rate = 1.0  # 1 request per second
        self.last_request_time = 0
        logger.info("📊 Basic Rate Limiter initialized: 1 req/sec")

    def acquire(self):
        """Rate limit acquisition (blocking)."""
        now = time.time()
        elapsed = now - self.last_request_time

        if elapsed < (1.0 / self.rate):
            wait_time = (1.0 / self.rate) - elapsed
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def backoff(self, response):
        """
        Reactive rate limiting based on response.

        For Titelive, 503 errors indicate server overload.
        """
        if response.status_code == 503:
            logger.warning("⚠️ Server returned 503, applying 15s backoff")
            time.sleep(15.0)


class TiteliveBurstRecoveryLimiter(BaseRateLimiter):
    """
    Burst-recovery rate limiter optimized for Titelive API.

    Pattern (from API probing):
    1. Burst Phase: 70 req/s for 2000 requests (~28 seconds)
    2. Recovery Phase: 15 second pause
    3. Sustained Phase: 30 req/s until next burst

    Effective throughput: ~72,000 req/hr (20x improvement over 1 req/sec)
    """

    def __init__(
        self,
        burst_rate: float = BURST_RATE,
        burst_size: int = BURST_SIZE,
        recovery_time: float = RECOVERY_TIME,
        sustained_rate: float = SUSTAINED_RATE,
    ):
        """
        Initialize burst-recovery rate limiter.

        Args:
            burst_rate: Requests per second during burst (default: 70)
            burst_size: Number of requests in burst phase (default: 2000)
            recovery_time: Pause duration after burst in seconds (default: 15)
            sustained_rate: Requests per second after recovery (default: 30)
        """
        self.burst_rate = burst_rate
        self.burst_size = burst_size
        self.recovery_time = recovery_time
        self.sustained_rate = sustained_rate

        # State tracking
        self.request_count = 0
        self.burst_start_time = None
        self.recovery_start_time = None
        self.last_request_time = 0

        logger.info(
            f"🚀 Burst-Recovery Rate Limiter initialized: "
            f"burst={burst_rate} req/s for {burst_size} requests, "
            f"recovery={recovery_time}s, sustained={sustained_rate} req/s"
        )

    def acquire(self):
        """
        Acquire rate limit permission (blocking).

        Implements 3-phase pattern:
        1. Burst: High throughput for initial requests
        2. Recovery: Mandatory cooldown
        3. Sustained: Moderate throughput
        """
        # Check if in recovery phase
        if self.recovery_start_time is not None:
            elapsed_recovery = time.time() - self.recovery_start_time
            remaining_recovery = self.recovery_time - elapsed_recovery

            if remaining_recovery > 0:
                logger.debug(f"⏸️ Recovery phase: waiting {remaining_recovery:.1f}s")
                time.sleep(remaining_recovery)

            # Exit recovery phase
            logger.info("✅ Recovery complete, resuming sustained phase")
            self.recovery_start_time = None
            self.request_count = 0  # Reset for sustained phase

        # Determine current phase and rate
        if self.request_count < self.burst_size:
            # Burst phase
            current_rate = self.burst_rate
            phase = "burst"

            if self.burst_start_time is None:
                self.burst_start_time = time.time()
                logger.info(f"🚀 Entering burst phase ({self.burst_size} requests)")
        else:
            # Sustained phase
            current_rate = self.sustained_rate
            phase = "sustained"

        # Apply rate limiting
        now = time.time()
        elapsed = now - self.last_request_time
        min_interval = 1.0 / current_rate

        if elapsed < min_interval:
            wait_time = min_interval - elapsed
            logger.debug(f"⏱️ [{phase}] Rate limiting: waiting {wait_time:.3f}s")
            time.sleep(wait_time)

        self.last_request_time = time.time()
        self.request_count += 1

        # Check if burst phase complete → enter recovery
        if self.request_count == self.burst_size:
            burst_duration = time.time() - self.burst_start_time
            logger.info(
                f"📊 Burst phase complete: {self.burst_size} requests "
                f"in {burst_duration:.1f}s ({self.burst_size / burst_duration:.1f} req/s)"
            )
            logger.info(f"⏸️ Entering recovery phase ({self.recovery_time}s)")
            self.recovery_start_time = time.time()
            self.burst_start_time = None

    def backoff(self, response):
        """
        Reactive rate limiting based on response.

        For Titelive, 503 errors indicate server overload.
        Force recovery phase if server is stressed.
        """
        if response.status_code == 503:
            logger.warning(
                "⚠️ Server returned 503 (overload), "
                f"forcing {self.recovery_time}s recovery"
            )
            self.recovery_start_time = time.time()
            # Don't reset request_count to preserve phase tracking
