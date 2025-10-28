import logging
from abc import ABC, abstractmethod
from typing import Optional

import requests
import httpx
import asyncio
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

    def __init__(
        self, rate_limiter: Optional[BaseRateLimiter] = None, max_retries: int = 3
    ):
        """
        Initialize synchronous HTTP client.

        Args:
            rate_limiter: Optional rate limiter instance
            max_retries: Maximum number of retries for rate limit (429) responses
        """
        super().__init__(rate_limiter)
        self.max_retries = max_retries

    def request(
        self,
        method: str,
        url: str,
        skip_rate_limit: bool = False,
        _retry_count: int = 0,
        **kwargs,
    ):
        """
        Perform HTTP request with rate limiting and retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            skip_rate_limit: If True, skip rate limiter acquire (used after backoff)
            _retry_count: Internal retry counter for rate limit retries
            **kwargs: Additional arguments passed to requests.request

        Returns:
            Response object or None on failure
        """
        try:
            # Only acquire rate limit token if not skipping (i.e., not retrying after backoff)
            if self.rate_limiter and not skip_rate_limit:
                self.rate_limiter.acquire()

            response = requests.request(method, url, **kwargs)

            # Handle 429 rate limit with backoff and retry
            if response.status_code == 429:
                if _retry_count >= self.max_retries:
                    logger.error(
                        f"Max retries ({self.max_retries}) reached for 429 response"
                    )
                    response.raise_for_status()  # Will raise HTTPError
                    return response

                if self.rate_limiter:
                    self.rate_limiter.backoff(response)
                    # Retry with skip_rate_limit=True to avoid double-waiting
                    return self.request(
                        method,
                        url,
                        skip_rate_limit=True,
                        _retry_count=_retry_count + 1,
                        **kwargs,
                    )
                else:
                    logger.error("Received 429 but no rate limiter configured")
                    response.raise_for_status()
                    return response

            # Handle 401 (let caller handle token refresh)
            if response.status_code == 401:
                logger.warning("Received 401 Unauthorized")
                return response

            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            # Re-raise HTTPError to preserve status code information (e.g., 404, 500)
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            raise
        except requests.RequestException as e:
            # For other request exceptions (network errors, etc.), return None
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
        self._restore_task: Optional[asyncio.Task] = None

    def _get_client(self) -> httpx.AsyncClient:
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

            client = self._get_client()
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
