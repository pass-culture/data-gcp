import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -----------------------------
# Abstract Rate Limiter
# -----------------------------
class BaseRateLimiter(ABC):
    """
    Abstract base class for rate limiter strategies.
    """

    @abstractmethod
    def acquire(self):
        """Acquire a slot before making a request."""
        pass

    @abstractmethod
    def backoff(self, response):
        """Handle backoff logic when receiving a rate-limit response."""
        pass

    def release(self):
        """
        Release resources after request completion.
        Optional method - only needed for some implementations (like async).
        """
        pass


class SimpleRateLimiter(BaseRateLimiter):
    """
    Simple rate limiter with fixed intervals.
    Useful as a default when no specific rate limiting is needed.
    """

    def __init__(self, requests_per_minute: int = 60):
        """
        Initialize simple rate limiter.

        Args:
            requests_per_minute: Maximum requests allowed per minute
        """
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60 / requests_per_minute if requests_per_minute > 0 else 0
        self.last_request_time = 0
        self._lock = threading.RLock()

        logger.info(f"SimpleRateLimiter initialized: {requests_per_minute} req/min")

    def acquire(self):
        """Wait if needed to respect minimum interval."""
        if self.min_interval == 0:
            return

        with self._lock:
            now = time.time()
            time_since_last = now - self.last_request_time

            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                logger.info(f"Rate limiting: waiting {wait_time:.2f}s")
                time.sleep(wait_time)

            self.last_request_time = time.time()

    def backoff(self, response):
        """Simple fixed backoff on rate limit."""
        logger.warning("Rate limited. Using fixed 60s backoff...")
        time.sleep(60)


# -----------------------------
# Sync Token Bucket
# -----------------------------
class TokenBucketRateLimiter(BaseRateLimiter):
    """
    Synchronous token bucket rate limiter.
    Allows for bursts, by controlling number of requests per time period.
    """

    def __init__(
        self,
        requests_per_minute: int = 60,
        burst_capacity: Optional[int] = None,
        window_size: int = 60,
    ):
        """
        Initialize token bucket rate limiter.

        Args:
            requests_per_minute: Average requests per minute
            burst_capacity: Maximum burst capacity (defaults to requests_per_minute)
            window_size: Time window in seconds
        """
        self.requests_per_minute = requests_per_minute
        self.burst_capacity = burst_capacity or requests_per_minute
        self.window_size = window_size

        # Token bucket state
        self._tokens = self.burst_capacity
        self._last_update = time.time()
        self._lock = threading.RLock()

        logger.info(
            f"TokenBucketRateLimiter initialized: {requests_per_minute} req/min, "
            f"burst={self.burst_capacity}"
        )

    def acquire(self):
        """Acquire a token, waiting if bucket is empty."""
        with self._lock:
            now = time.time()

            # Add tokens based on elapsed time
            elapsed = now - self._last_update
            tokens_to_add = elapsed * (self.requests_per_minute / self.window_size)
            self._tokens = min(self.burst_capacity, self._tokens + tokens_to_add)
            self._last_update = now

            # Check if we have tokens
            if self._tokens >= 1:
                self._tokens -= 1
                return

            # Calculate wait time
            wait_time = (1 - self._tokens) / (
                self.requests_per_minute / self.window_size
            )
            logger.info(f"Token bucket empty. Waiting {wait_time:.2f}s...")
            time.sleep(wait_time)

            # Update after waiting
            self._tokens = 0
            self._last_update = time.time()

    def backoff(self, response):
        """Handle rate limit with exponential backoff."""
        # Try to extract wait time from headers
        wait_time = 60  # default

        if hasattr(response, "headers"):
            # Common rate limit headers
            reset_time = response.headers.get("Retry-After")
            if reset_time:
                try:
                    wait_time = float(reset_time)
                except ValueError:
                    pass

        logger.warning(f"Rate limited. Waiting {wait_time}s...")
        time.sleep(wait_time)

        # Reset token bucket after backoff
        with self._lock:
            self._tokens = self.burst_capacity
            self._last_update = time.time()


# -----------------------------
# Async Token Bucket
# -----------------------------
class AsyncTokenBucketRateLimiter(BaseRateLimiter):
    """
    Asynchronous token bucket rate limiter.
    Supports semaphore-based concurrency control and async backoff.
    """

    def __init__(
        self,
        requests_per_minute: int = 60,
        burst_capacity: Optional[int] = None,
        max_concurrent: int = 10,
        window_size: int = 60,
    ):
        """
        Initialize async token bucket rate limiter.

        Args:
            requests_per_minute: Average requests per minute
            burst_capacity: Maximum burst capacity
            max_concurrent: Maximum concurrent requests
            window_size: Time window in seconds
        """
        self.requests_per_minute = requests_per_minute
        self.burst_capacity = burst_capacity or requests_per_minute
        self.max_concurrent = max_concurrent
        self.window_size = window_size

        # Async synchronization
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()

        # Token bucket state
        self._tokens = self.burst_capacity
        self._last_update = time.time()

        logger.info(
            f"AsyncTokenBucketRateLimiter initialized: {requests_per_minute} req/min, "
            f"burst={self.burst_capacity}, concurrent={max_concurrent}"
        )

    async def acquire(self):
        """Acquire permission for async request."""
        # First acquire semaphore for concurrency control
        await self._semaphore.acquire()

        async with self._lock:
            now = time.time()

            # Add tokens based on elapsed time
            elapsed = now - self._last_update
            tokens_to_add = elapsed * (self.requests_per_minute / self.window_size)
            self._tokens = min(self.burst_capacity, self._tokens + tokens_to_add)
            self._last_update = now

            # Check if we have tokens
            if self._tokens >= 1:
                self._tokens -= 1
                return

            # Calculate wait time
            wait_time = (1 - self._tokens) / (
                self.requests_per_minute / self.window_size
            )
            logger.info(f"[Async] Token bucket empty. Waiting {wait_time:.2f}s...")

        # Wait outside the lock
        await asyncio.sleep(wait_time)

        # Update after waiting
        async with self._lock:
            self._tokens = 0
            self._last_update = time.time()

    async def backoff(self, response):
        """Handle async rate limit backoff."""
        # Try to extract wait time from headers
        wait_time = 60  # default

        if hasattr(response, "headers"):
            reset_time = response.headers.get("Retry-After")
            if reset_time:
                try:
                    wait_time = float(reset_time)
                except ValueError:
                    pass

        logger.warning(f"[Async] Rate limited. Waiting {wait_time}s...")
        await asyncio.sleep(wait_time)

        # Reset token bucket after backoff
        async with self._lock:
            self._tokens = self.burst_capacity
            self._last_update = time.time()

    def release(self):
        """Release semaphore after request completion."""
        self._semaphore.release()


class NoOpRateLimiter(BaseRateLimiter):
    """
    No-operation rate limiter for testing or when no rate limiting is needed.
    """

    def acquire(self):
        """No-op acquire."""
        pass

    def backoff(self, response):
        """Basic backoff even when rate limiting is disabled."""
        logger.warning("Rate limited (no-op limiter). Using minimal backoff...")
        time.sleep(1)

    def release(self):
        """No-op release."""
        pass
