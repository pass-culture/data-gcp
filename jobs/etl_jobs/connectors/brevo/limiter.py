import asyncio
import logging
import time

from http_tools.rate_limiters import AsyncBaseRateLimiter, SyncBaseRateLimiter

logger = logging.getLogger(__name__)


def parse_brevo_reset(response) -> float:
    """Helper to parse Brevo's specific reset header."""
    return float(response.headers.get("x-sib-ratelimit-reset", "10"))


class SyncBrevoRateLimiter(SyncBaseRateLimiter):
    def acquire(self):
        pass  # Brevo uses dynamic limits; we only react to 429s

    def backoff(self, response):
        wait = parse_brevo_reset(response)
        logger.warning(f"Brevo Rate Limit: Sleeping {wait}s")
        time.sleep(wait)


class AsyncBrevoRateLimiter(AsyncBaseRateLimiter):
    def __init__(self, max_concurrent: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def acquire(self):
        await self.semaphore.acquire()

    def release(self):
        self.semaphore.release()

    async def backoff(self, response):
        wait = parse_brevo_reset(response)
        logger.warning(f"[Async] Brevo Rate Limit: Sleeping {wait}s")
        await asyncio.sleep(wait)
