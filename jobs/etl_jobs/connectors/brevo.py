import asyncio
import logging
import random
import time
from typing import Optional

from http_tools.clients import SyncHttpClient, AsyncHttpClient
from http_tools.rate_limiters import BaseRateLimiter

BASE_URL = "https://api.brevo.com/v3"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ========================================
# Brevo-Specific Rate Limiters
# ========================================


class SyncBrevoHeaderRateLimiter(BaseRateLimiter):
    """
    Brevo-specific rate limiter using headers:
    - x-sib-ratelimit-limit
    - x-sib-ratelimit-remaining
    - x-sib-ratelimit-reset
    """

    def acquire(self):
        # No pre-request throttling — Brevo rate is dynamic
        pass

    def backoff(self, response):
        try:
            reset = float(response.headers.get("x-sib-ratelimit-reset", "10"))
            logger.warning(
                f"Rate limited. Waiting {reset:.2f}s based on x-sib-ratelimit-reset header..."
            )
            time.sleep(reset)
        except Exception:
            logger.warning("Fallback backoff: 10s")
            time.sleep(10)


class AsyncBrevoHeaderRateLimiter(BaseRateLimiter):
    """
    Async Brevo-specific rate limiter with concurrency control to prevent thundering herd.
    Uses semaphore to limit concurrent requests and implements jittered backoff.
    """

    def __init__(self, max_concurrent: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.backoff_event = asyncio.Event()
        self.backoff_event.set()  # Initially not in backoff
        self.backoff_lock = asyncio.Lock()
        self.request_interval = 0.2  # 200ms between requests
        self.last_request_time = 0
        self._restore_task: Optional[asyncio.Task] = None

    async def acquire(self):
        """Acquire permission to make a request."""
        # Wait if we're in global backoff
        await self.backoff_event.wait()

        # Acquire semaphore for concurrency control
        await self.semaphore.acquire()

        # Add small delay between requests to avoid bursts
        async with self.backoff_lock:
            now = time.time()
            time_since_last = now - self.last_request_time
            if time_since_last < self.request_interval:
                await asyncio.sleep(self.request_interval - time_since_last)
            self.last_request_time = time.time()

    def release(self):
        """Release the semaphore after request completion."""
        self.semaphore.release()

    async def backoff(self, response):
        """Handle rate limit with jittered backoff to prevent thundering herd."""
        async with self.backoff_lock:
            if not self.backoff_event.is_set():
                # Already in backoff, don't reset
                return

            # Clear the event to block new requests
            self.backoff_event.clear()

            try:
                reset_time = float(response.headers.get("x-sib-ratelimit-reset", "10"))
                # Add jitter: 0-10% additional wait time to spread out retries

                jitter = reset_time * random.uniform(0, 0.01)
                total_wait = reset_time + jitter

                logger.warning(
                    f"[Async] Rate limited. Waiting {total_wait:.2f}s "
                    f"(base: {reset_time:.2f}s + jitter: {jitter:.2f}s)"
                )

                await asyncio.sleep(total_wait)

                # Gradually release requests instead of all at once
                self.request_interval = 1.0  # Increase interval after backoff

            except Exception:
                logger.warning("[Async] Fallback backoff: 10s")
                await asyncio.sleep(10)
            finally:
                # Resume accepting requests
                self.backoff_event.set()

                # Gradually decrease interval back to normal
                self._restore_task = asyncio.create_task(self._gradually_restore_rate())

    async def _gradually_restore_rate(self):
        """Gradually restore request rate to normal after backoff."""
        steps = 10
        for _ in range(steps):
            await asyncio.sleep(2)  # Wait 2 seconds between steps
            self.request_interval = max(0.2, self.request_interval - 0.08)


# ========================================
# Brevo API Connectors
# ========================================


class BrevoConnector:
    """
    Brevo API wrapper using SyncHttpClient for interacting with Brevo endpoints.
    """

    def __init__(self, api_key: str, client: Optional[SyncHttpClient] = None):
        self.api_key = api_key
        self.client = client or SyncHttpClient()
        self.headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @classmethod
    def create_with_rate_limiter(cls, api_key: str, **client_kwargs):
        """
        Factory method to create BrevoConnector with appropriate rate limiter.

        Args:
            api_key: Brevo API key
            **client_kwargs: Additional arguments for HTTP client

        Returns:
            BrevoConnector instance with pre-configured rate limiter
        """
        rate_limiter = SyncBrevoHeaderRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter, **client_kwargs)
        return cls(api_key=api_key, client=client)

    def get_email_campaigns(
        self,
        status: str = "sent",
        limit: int = 50,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
    ):
        url = f"{BASE_URL}/emailCampaigns"
        params = {
            "status": status,
            "limit": limit,
            "offset": offset,
            "statistics": "globalStats",
        }
        if start_date:
            params["startDate"] = start_date

        if end_date:
            params["endDate"] = end_date

        return self.client.request("GET", url, headers=self.headers, params=params)

    def get_smtp_templates(self, active_only: bool = True, offset: int = 0):
        url = f"{BASE_URL}/smtp/templates"
        params = {"templateStatus": str(active_only).lower(), "offset": offset}
        return self.client.request("GET", url, headers=self.headers, params=params)

    def get_email_event_report(
        self,
        template_id: int,
        event: str,
        start_date: str,
        end_date: str,
        offset: int = 0,
    ):
        url = f"{BASE_URL}/smtp/statistics/events"
        params = {
            "templateId": template_id,
            "event": event,
            "startDate": start_date,
            "endDate": end_date,
            "offset": offset,
        }
        return self.client.request("GET", url, headers=self.headers, params=params)


class AsyncBrevoConnector:
    """
    Brevo API wrapper using AsyncHttpClient for non-blocking API interactions.
    """

    def __init__(self, api_key: str, client: Optional[AsyncHttpClient] = None):
        self.api_key = api_key
        self.client = client or AsyncHttpClient()
        self.headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @classmethod
    def create_with_rate_limiter(
        cls, api_key: str, max_concurrent: int = 5, **client_kwargs
    ):
        """
        Factory method to create AsyncBrevoConnector with appropriate rate limiter.

        Args:
            api_key: Brevo API key
            max_concurrent: Maximum concurrent requests
            **client_kwargs: Additional arguments for HTTP client

        Returns:
            AsyncBrevoConnector instance with pre-configured rate limiter
        """
        rate_limiter = AsyncBrevoHeaderRateLimiter(max_concurrent=max_concurrent)
        client = AsyncHttpClient(rate_limiter=rate_limiter, **client_kwargs)
        return cls(api_key=api_key, client=client)

    async def get_email_campaigns(
        self, status: str = "sent", limit: int = 50, offset: int = 0
    ):
        url = f"{BASE_URL}/emailCampaigns"
        params = {"status": status, "limit": limit, "offset": offset}
        try:
            response = await self.client.request(
                "GET", url, headers=self.headers, params=params
            )
            return response
        finally:
            # Release rate limiter after request completes
            if self.client.rate_limiter and hasattr(
                self.client.rate_limiter, "release"
            ):
                self.client.rate_limiter.release()

    async def get_smtp_templates(self, active_only: bool = True, offset: int = 0):
        url = f"{BASE_URL}/smtp/templates"
        params = {"templateStatus": str(active_only).lower(), "offset": offset}
        try:
            response = await self.client.request(
                "GET", url, headers=self.headers, params=params
            )
            return response
        finally:
            # Release rate limiter after request completes
            if self.client.rate_limiter and hasattr(
                self.client.rate_limiter, "release"
            ):
                self.client.rate_limiter.release()

    async def get_email_event_report(
        self,
        template_id: int,
        event: str,
        start_date: str,
        end_date: str,
        offset: int = 0,
    ):
        url = f"{BASE_URL}/smtp/statistics/events"
        params = {
            "templateId": template_id,
            "event": event,
            "startDate": start_date,
            "endDate": end_date,
            "offset": offset,
        }
        try:
            response = await self.client.request(
                "GET", url, headers=self.headers, params=params
            )
            return response
        finally:
            # Release rate limiter after request completes
            if self.client.rate_limiter and hasattr(
                self.client.rate_limiter, "release"
            ):
                self.client.rate_limiter.release()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - close underlying client."""
        if hasattr(self.client, "close"):
            await self.client.close()


# ========================================
# Convenience Functions
# ========================================


def create_brevo_connector(api_key: str, **kwargs) -> BrevoConnector:
    """
    Create a sync Brevo connector with recommended rate limiter.

    Args:
        api_key: Brevo API key
        **kwargs: Additional HTTP client configuration

    Returns:
        Configured BrevoConnector
    """
    return BrevoConnector.create_with_rate_limiter(api_key, **kwargs)


def create_async_brevo_connector(
    api_key: str, max_concurrent: int = 5, **kwargs
) -> AsyncBrevoConnector:
    """
    Create an async Brevo connector with recommended rate limiter.

    Args:
        api_key: Brevo API key
        max_concurrent: Maximum concurrent requests
        **kwargs: Additional HTTP client configuration

    Returns:
        Configured AsyncBrevoConnector
    """
    return AsyncBrevoConnector.create_with_rate_limiter(
        api_key, max_concurrent, **kwargs
    )
