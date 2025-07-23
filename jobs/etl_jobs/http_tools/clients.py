import logging
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

import requests
import httpx
from http_tools.rate_limiters import BaseRateLimiter

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -----------------------------
# Base HTTP Client (abstract)
# -----------------------------
class BaseHttpClient(ABC):
    """
    Abstract base class for HTTP clients.
    Supports pluggable rate limiter strategy.
    """

    def __init__(self, rate_limiter: Optional[BaseRateLimiter] = None):
        self.rate_limiter = rate_limiter

    def set_rate_limiter(self, limiter: BaseRateLimiter):
        """Set or update the rate limiter instance."""
        self.rate_limiter = limiter

    @abstractmethod
    def request(self, method: str, url: str, **kwargs):
        """Perform a request with the given HTTP method and URL."""
        pass


# -----------------------------
# Sync HTTP Client
# -----------------------------
class SyncHttpClient(BaseHttpClient):
    """
    Synchronous HTTP client with optional rate limiting and error handling.
    """

    def request(self, method: str, url: str, **kwargs):
        try:
            if self.rate_limiter:
                self.rate_limiter.acquire()

            response = requests.request(method, url, **kwargs)

            if response.status_code == 429 and self.rate_limiter:
                self.rate_limiter.backoff(response)
                return self.request(method, url, **kwargs)  # Retry

            response.raise_for_status()
            return response

        except requests.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
            return None


# -----------------------------
# Async HTTP Client
# -----------------------------
class AsyncHttpClient(BaseHttpClient):
    """
    Asynchronous HTTP client using httpx.AsyncClient with rate limiting.
    Designed to be memory-safe and exception-resilient.
    """

    def __init__(self, rate_limiter: Optional[BaseRateLimiter] = None):
        super().__init__(rate_limiter)
        self._client = None

    async def _get_client(self):
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),  # 30 second timeout
                limits=httpx.Limits(
                    max_keepalive_connections=20,
                    max_connections=50,
                ),
            )
        return self._client

    async def request(self, method: str, url: str, **kwargs):
        """
        Make an async HTTP request with rate limiting.
        The rate limiter's semaphore is NOT automatically released here
        to allow the caller to control when it's safe to make the next request.
        """
        acquired = False
        try:
            if self.rate_limiter:
                await self.rate_limiter.acquire()
                acquired = True

            client = await self._get_client()
            response = await client.request(method, url, **kwargs)

            if response.status_code == 429 and self.rate_limiter:
                # Release semaphore before backoff
                if acquired and hasattr(self.rate_limiter, "release"):
                    self.rate_limiter.release()
                    acquired = False

                await self.rate_limiter.backoff(response)
                return await self.request(method, url, **kwargs)  # Retry

            response.raise_for_status()
            return response

        except httpx.HTTPError as e:
            logger.error(f"Async HTTP request failed: {e}")
            # Release semaphore on error
            if acquired and self.rate_limiter and hasattr(self.rate_limiter, "release"):
                self.rate_limiter.release()
            return None
        except Exception as e:
            logger.error(f"Unexpected error in async request: {e}")
            # Release semaphore on any error
            if acquired and self.rate_limiter and hasattr(self.rate_limiter, "release"):
                self.rate_limiter.release()
            raise

    async def close(self):
        """Close the underlying async HTTP client to free memory."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        # Don't create client here, let it be created on first request
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
