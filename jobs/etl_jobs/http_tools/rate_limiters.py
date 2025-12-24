import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque

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


# -----------------------------
# Sync Token Bucket
# -----------------------------
class SyncTokenBucketRateLimiter(BaseRateLimiter):
    """
    Synchronous token bucket rate limiter.
    Controls number of requests per time period.
    """

    def __init__(self, calls: int, period: int, default_backoff: int = 10):
        self.calls = calls
        self.period = period
        self.timestamps = deque()
        self.default_backoff = default_backoff

    def acquire(self):
        while len(self.timestamps) >= self.calls:
            now = time.time()
            if now - self.timestamps[0] > self.period:
                self.timestamps.popleft()
            else:
                sleep_time = self.period - (now - self.timestamps[0])
                logger.warning(f"Rate limit reached. Sleeping {sleep_time:.2f}s...")
                time.sleep(sleep_time)
        self.timestamps.append(time.time())

    def backoff(self, response):
        header = response.headers.get("Retry-After")
        if header is None:
            logger.warning(
                f"Retry-After header missing; defaulting to {self.default_backoff}s backoff"
            )
            retry_after = self.default_backoff
        else:
            try:
                retry_after = int(header)
            except ValueError:
                logger.warning(
                    f"Invalid Retry-After header '{header}'; defaulting to {self.default_backoff}s backoff"
                )
                retry_after = self.default_backoff
        logger.warning(f"Received 429. Backing off for {retry_after}s…")
        time.sleep(retry_after)


# -----------------------------
# Async Token Bucket
# -----------------------------
class AsyncTokenBucketRateLimiter(BaseRateLimiter):
    """
    Asynchronous token bucket rate limiter.
    Uses asyncio locks and sleeps for non-blocking behavior.
    """

    def __init__(self, calls: int, period: int, default_backoff: int = 10):
        self.calls = calls
        self.period = period
        self.timestamps = deque()
        self.lock = asyncio.Lock()
        self.default_backoff = default_backoff

    async def acquire(self):
        async with self.lock:
            while len(self.timestamps) >= self.calls:
                now = time.time()
                if now - self.timestamps[0] > self.period:
                    self.timestamps.popleft()
                else:
                    wait = self.period - (now - self.timestamps[0])
                    logger.warning(
                        f"[Async] Rate limit reached. Sleeping {wait:.2f}s..."
                    )
                    await asyncio.sleep(wait)
            self.timestamps.append(time.time())

    async def backoff(self, response):
        header = response.headers.get("Retry-After")
        if header is None:
            logger.warning(
                f"[Async] Retry-After header missing; defaulting to {self.default_backoff}s backoff"
            )
            retry_after = self.default_backoff
        else:
            try:
                retry_after = int(header)
            except ValueError:
                logger.warning(
                    f"[Async] Invalid Retry-After header '{header}'; defaulting to {self.default_backoff}s backoff"
                )
                retry_after = self.default_backoff
        logger.warning(f"[Async] Received 429. Backing off for {retry_after}s…")
        await asyncio.sleep(retry_after)
