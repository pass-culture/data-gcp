import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

# Set up logging
ENV_LOG_LEVEL = os.getenv("CLIENT_LOG_LEVEL", "INFO").upper()
LIMITER_LOG_LEVEL = getattr(logging, ENV_LOG_LEVEL, logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(LIMITER_LOG_LEVEL)


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
        logger.debug(f"Could not parse Retry-After header: '{header}'. Using default.")
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

    def _log_event(
        self, icon: str, event_type: str, message: str, level: int = logging.DEBUG
    ):
        """Standardized format: ICON [ClassName] EVENT: Message"""
        limiter_name = self.__class__.__name__
        logger.log(level, f"{icon} [{limiter_name}] {event_type.upper()}: {message}")


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
                # USE BASE LOGGING
                self._log_event(
                    "⏳",
                    "Throttle",
                    f"Proactive rate limit. Sleeping {sleep_time:.2f}s",
                )
                time.sleep(sleep_time)
        self.timestamps.append(time.time())

    def backoff(self, response):
        header_val = response.headers.get("Retry-After")
        retry_after = parse_retry_after(response, self.default_backoff)
        source = "Retry-After header" if header_val else "default backoff"

        # USE BASE LOGGING
        self._log_event(
            "😴",
            "Backoff",
            f"Received 429. Waiting {retry_after}s (Source: {source})",
            logging.WARNING,
        )
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
        self.semaphore = asyncio.Semaphore(max_concurrent) if max_concurrent else None

    async def acquire(self):
        if self.semaphore:
            if self.semaphore.locked():
                # USE BASE LOGGING
                self._log_event(
                    "🚧",
                    "Concurrency",
                    "Max concurrent slots reached. Waiting...",
                    logging.DEBUG,
                )
            await self.semaphore.acquire()

        async with self.lock:
            while len(self.timestamps) >= self.calls:
                now = time.time()
                if now - self.timestamps[0] > self.period:
                    self.timestamps.popleft()
                else:
                    wait = self.period - (now - self.timestamps[0])
                    # USE BASE LOGGING
                    self._log_event(
                        "⏳",
                        "Throttle",
                        f"Proactive rate limit. Sleeping {wait:.2f}s",
                        logging.INFO,
                    )
                    await asyncio.sleep(wait)
            self.timestamps.append(time.time())

    def release(self):
        if self.semaphore:
            self.semaphore.release()

    async def backoff(self, response):
        header_val = response.headers.get("Retry-After")
        retry_after = parse_retry_after(response, self.default_backoff)
        source = "Retry-After header" if header_val else "default backoff"

        # USE BASE LOGGING
        self._log_event(
            "😴",
            "Backoff",
            f"Received 429. Waiting {retry_after}s (Source: {source})",
            logging.WARNING,
        )
        await asyncio.sleep(retry_after)
