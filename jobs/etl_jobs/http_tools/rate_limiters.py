import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)


# -----------------------------
# Helper for Header Parsing
# -----------------------------
def parse_retry_after(response, default_backoff: int) -> int:
    """Extracts retry delay from response headers."""
    header = response.headers.get("Retry-After")
    if header is None:
        return default_backoff
    try:
        return int(header)
    except ValueError:
        # Some APIs send a HTTP-date string instead of seconds
        return default_backoff


# -----------------------------
# Abstract Base Classes
# -----------------------------


class BaseRateLimiter(ABC):
    @abstractmethod
    def backoff(self, response):
        pass

    def release(self):
        """Optional: Release resources/semaphores after a request."""
        pass


class SyncBaseRateLimiter(BaseRateLimiter):
    @abstractmethod
    def acquire(self):
        pass


class AsyncBaseRateLimiter(BaseRateLimiter):
    @abstractmethod
    async def acquire(self):
        pass

    @abstractmethod
    async def backoff(self, response):
        pass


# -----------------------------
# Sync Token Bucket
# -----------------------------
class SyncTokenBucketRateLimiter(SyncBaseRateLimiter):
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
        retry_after = parse_retry_after(response, self.default_backoff)
        logger.warning(f"Received 429. Backing off for {retry_after}s…")
        time.sleep(retry_after)


# -----------------------------
# Async Token Bucket
# -----------------------------
class AsyncTokenBucketRateLimiter(AsyncBaseRateLimiter):
    def __init__(
        self,
        calls: int,
        period: int,
        default_backoff: int = 10,
        max_concurrent: Optional[int] = None,
    ):
        self.calls = calls
        self.period = period
        self.timestamps = deque()
        self.lock = asyncio.Lock()
        self.default_backoff = default_backoff
        # Optional: Add a semaphore if you want to limit concurrency AND rate
        self.semaphore = asyncio.Semaphore(max_concurrent) if max_concurrent else None

    async def acquire(self):
        # 1. Handle Concurrency Limit (if set)
        if self.semaphore:
            await self.semaphore.acquire()

        # 2. Handle Rate Limit (calls per period)
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

    def release(self):
        """Releases the concurrency semaphore."""
        if self.semaphore:
            self.semaphore.release()

    async def backoff(self, response):
        retry_after = parse_retry_after(response, self.default_backoff)
        logger.warning(f"[Async] Received 429. Backing off for {retry_after}s…")
        await asyncio.sleep(retry_after)
