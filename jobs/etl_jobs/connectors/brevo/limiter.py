import asyncio
import logging
import os
import time

from http_tools.rate_limiters import AsyncBaseRateLimiter, SyncBaseRateLimiter

ENV_LOG_LEVEL = os.getenv("CLIENT_LOG_LEVEL", "INFO").upper()
LIMITER_LOG_LEVEL = getattr(logging, ENV_LOG_LEVEL, logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(LIMITER_LOG_LEVEL)


def parse_brevo_reset(response) -> float:
    """Helper to parse Brevo's specific reset header."""
    # Brevo sends the number of seconds until the limit resets
    return float(response.headers.get("x-sib-ratelimit-reset", "10"))


class SyncBrevoRateLimiter(SyncBaseRateLimiter):
    """Sync limiter using Brevo's dynamic reset headers and standardized logging."""

    def acquire(self):
        # Brevo uses dynamic limits; no proactive throttling required here.
        pass

    def backoff(self, response):
        """Standardized backoff for Brevo using base logging helper."""
        wait = parse_brevo_reset(response)

        # USE BASE LOGGING: Standard icon 😴 and event type BACKOFF
        self._log_event(
            "😴",
            "Backoff",
            f"Brevo limit hit. Waiting {wait}s (Source: x-sib-ratelimit-reset)",
            logging.WARNING,
        )
        time.sleep(wait)


class AsyncBrevoRateLimiter(AsyncBaseRateLimiter):
    """Async limiter using Brevo's dynamic reset headers and standardized logging."""

    def __init__(self, max_concurrent: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def acquire(self):
        """Handles concurrency control with standardized logging."""
        if self.semaphore.locked():
            # USE BASE LOGGING: Standard icon ⏳ and event type CONCURRENCY
            self._log_event(
                "⏳",
                "Concurrency",
                "Max concurrent slots reached. Waiting for a slot...",
                logging.DEBUG,
            )
        await self.semaphore.acquire()

    def release(self):
        """Required to free up slots for the next async request."""
        self.semaphore.release()

    async def backoff(self, response):
        """Standardized async backoff for Brevo."""
        wait = parse_brevo_reset(response)

        # USE BASE LOGGING
        self._log_event(
            "😴",
            "Backoff",
            f"Brevo limit hit. Waiting {wait}s (Source: x-sib-ratelimit-reset)",
            logging.WARNING,
        )
        await asyncio.sleep(wait)
