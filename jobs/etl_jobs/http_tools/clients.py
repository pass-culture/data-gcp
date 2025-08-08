import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional, List

import requests
import httpx
import asyncio
from http_tools.rate_limiters import BaseRateLimiter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    Synchronous HTTP client with retry logic, optional rate limiting and error handling.
    """

    def __init__(
        self,
        rate_limiter: Optional[BaseRateLimiter] = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_status_codes: Optional[List[int]] = None,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize enhanced sync HTTP client.

        Args:
            rate_limiter: Rate limiter instance
            timeout: Request timeout in seconds
            max_retries: Maximum retries for server errors (excludes 429)
            retry_status_codes: HTTP status codes to retry on (excludes 429)
            backoff_factor: Exponential backoff factor for retries
        """
        super().__init__(rate_limiter)
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_status_codes = retry_status_codes or [
            500,
            502,
            503,
            504,
        ]  # Excludes 429
        self.backoff_factor = backoff_factor
        self._setup_session()

        # Request tracking
        self._request_count = 0

    def _setup_session(self):
        """Set up requests session with retry configuration for server errors."""
        self.session = requests.Session()

        # Configure automatic retries for server errors (excludes 429 - rate limiter handles that)
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.retry_status_codes,  # Does not include 429
            allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def request(self, method: str, url: str, **kwargs):
        """
        Make HTTP request with retry logic, rate limiting and error handling.
        Rate limiter release is controlled by the rate limiter itself.
        """
        self._request_count += 1
        request_id = self._request_count

        # Set default timeout
        kwargs.setdefault("timeout", self.timeout)

        # Track rate limit retries to prevent infinite recursion
        max_rate_limit_retries = 3
        rate_limit_retry_count = kwargs.pop("_rate_limit_retry_count", 0)

        # Log request details (sanitized)
        self._log_request(
            request_id, method, url, kwargs.get("params"), kwargs.get("headers")
        )

        start_time = time.time()

        try:
            # Apply rate limiting before request
            if self.rate_limiter:
                self.rate_limiter.acquire()

            # Make the request (session handles server error retries automatically)
            response = self.session.request(method, url, **kwargs)

            duration = time.time() - start_time
            self._log_response(request_id, method, url, response, duration)

            # Handle rate limiting (429) with controlled retries
            if response.status_code == 429:
                if rate_limit_retry_count >= max_rate_limit_retries:
                    logger.error(
                        f"Request {request_id}: Max rate limit retries ({max_rate_limit_retries}) exceeded"
                    )
                    return response  # Return 429 response instead of continuing

                if self.rate_limiter:
                    logger.warning(
                        f"Request {request_id}: Rate limited, applying backoff (retry {rate_limit_retry_count + 1})"
                    )
                    self.rate_limiter.backoff(response)

                    # Retry with incremented count
                    kwargs["_rate_limit_retry_count"] = rate_limit_retry_count + 1
                    return self.request(method, url, **kwargs)  # Retry

            # Log successful or error responses
            if response.ok:
                logger.debug(f"Request {request_id}: Success ({response.status_code})")
            else:
                logger.warning(
                    f"Request {request_id}: HTTP error {response.status_code} - {response.reason}"
                )

            return response

        except requests.exceptions.Timeout as e:
            logger.error(
                f"Request {request_id}: Timeout after {self.timeout}s - {method} {url} - {e}"
            )
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(
                f"Request {request_id}: Connection error - {method} {url} - {e}"
            )
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request {request_id}: Request failed - {method} {url} - {e}")
            return None
        except Exception as e:
            logger.error(
                f"Request {request_id}: Unexpected error - {method} {url} - {e}"
            )
            return None
        finally:
            # Rate limiter controls its own release
            # Only release if rate limiter has a release method (for compatibility)
            if self.rate_limiter and hasattr(self.rate_limiter, "release"):
                try:
                    self.rate_limiter.release()
                except Exception as e:
                    logger.debug(
                        f"Request {request_id}: Rate limiter release failed: {e}"
                    )

    def _log_request(
        self, request_id: int, method: str, url: str, params: dict, headers: dict
    ):
        """Log request details with sensitive data sanitization."""
        # Sanitize sensitive parameters and headers
        safe_params = {}
        if params:
            safe_params = {
                k: v
                for k, v in params.items()
                if not any(
                    sensitive in k.lower()
                    for sensitive in ["token", "key", "secret", "password"]
                )
            }

        safe_headers = {}
        if headers:
            safe_headers = {
                k: (
                    "***REDACTED***"
                    if any(
                        sensitive in k.lower()
                        for sensitive in ["token", "key", "secret", "auth"]
                    )
                    else v
                )
                for k, v in headers.items()
            }

        logger.debug(f"Request {request_id}: {method.upper()} {url}")
        if safe_params:
            logger.debug(f"Request {request_id}: Params {safe_params}")
        if safe_headers:
            logger.debug(f"Request {request_id}: Headers {safe_headers}")

    def _log_response(
        self,
        request_id: int,
        method: str,
        url: str,
        response: requests.Response,
        duration: float,
    ):
        """Log response details."""
        logger.debug(
            f"Request {request_id}: {method.upper()} {url} -> {response.status_code} in {duration:.2f}s"
        )

        # Log rate limit headers if present (for debugging)
        if logger.isEnabledFor(logging.DEBUG):
            rate_headers = {
                k: v
                for k, v in response.headers.items()
                if any(
                    rate_key in k.lower()
                    for rate_key in ["rate", "limit", "remaining", "reset"]
                )
            }
            if rate_headers:
                logger.debug(f"Request {request_id}: Rate limit headers {rate_headers}")

    def get_stats(self):
        """Get client statistics."""
        return {
            "total_requests": self._request_count,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
        }


# -----------------------------
# Async HTTP Client
# -----------------------------
class AsyncHttpClient(BaseHttpClient):
    """
    Asynchronous HTTP client using httpx.AsyncClient with rate limiting.
    Enhanced with configurable retry logic and better error handling.
    Designed to be memory-safe and exception-resilient.

    The rate limiter's semaphore is NOT automatically released here
    to allow the caller to control when it's safe to make the next request.
    """

    def __init__(
        self,
        rate_limiter: Optional[BaseRateLimiter] = None,
        timeout: float = 30.0,
        max_keepalive_connections: int = 20,
        max_connections: int = 50,
        max_retries: int = 3,
        retry_status_codes: Optional[List[int]] = None,
        backoff_factor: float = 2.0,
    ):
        """
        Initialize enhanced async HTTP client.

        Args:
            rate_limiter: Rate limiter instance
            timeout: Request timeout in seconds
            max_keepalive_connections: Max persistent connections
            max_connections: Total connection limit
            max_retries: Maximum retries for server errors (excludes 429)
            retry_status_codes: HTTP status codes to retry on (excludes 429)
            backoff_factor: Exponential backoff factor for retries
        """
        super().__init__(rate_limiter)
        self.timeout = timeout
        self.max_keepalive_connections = max_keepalive_connections
        self.max_connections = max_connections
        self.max_retries = max_retries
        self.retry_status_codes = set(
            retry_status_codes or [500, 502, 503, 504]
        )  # Excludes 429
        self.backoff_factor = backoff_factor
        self._client = None
        self._restore_task: Optional[asyncio.Task] = None

        # Request tracking
        self._request_count = 0

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create the httpx client with proper configuration."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                limits=httpx.Limits(
                    max_keepalive_connections=self.max_keepalive_connections,
                    max_connections=self.max_connections,
                ),
            )
        return self._client

    async def request(self, method: str, url: str, **kwargs):
        """
        Make an async HTTP request with rate limiting and enhanced error handling.
        The rate limiter's semaphore is NOT automatically released here
        to allow the caller to control when it's safe to make the next request.
        """
        self._request_count += 1
        request_id = self._request_count

        # Track rate limit retries to prevent infinite recursion
        max_rate_limit_retries = 3
        rate_limit_retry_count = kwargs.pop("_rate_limit_retry_count", 0)

        # Track if we acquired the rate limiter
        acquired = False

        # Log request details (sanitized)
        self._log_request(
            request_id, method, url, kwargs.get("params"), kwargs.get("headers")
        )

        try:
            # Apply rate limiting before request
            if self.rate_limiter:
                # Handle both sync and async acquire methods
                if hasattr(self.rate_limiter, "acquire"):
                    if asyncio.iscoroutinefunction(self.rate_limiter.acquire):
                        await self.rate_limiter.acquire()
                    else:
                        self.rate_limiter.acquire()
                    acquired = True

            # Retry logic for server errors (excludes 429)
            for attempt in range(self.max_retries + 1):
                start_time = time.time()

                try:
                    # Make the request
                    client = self._get_client()
                    response = await client.request(method, url, **kwargs)

                    duration = time.time() - start_time
                    self._log_response(
                        request_id, method, url, response, duration, attempt
                    )

                    # Handle rate limiting (429) with controlled retries
                    if response.status_code == 429:
                        if rate_limit_retry_count >= max_rate_limit_retries:
                            logger.error(
                                f"Request {request_id}: Max rate limit retries ({max_rate_limit_retries}) exceeded"
                            )
                            return response  # Return 429 response instead of continuing

                        if self.rate_limiter:
                            logger.warning(
                                f"Request {request_id}: Rate limited, applying backoff (retry {rate_limit_retry_count + 1})"
                            )

                            # PRESERVED BEHAVIOR: Don't release semaphore - let rate limiter handle it
                            # Apply backoff (handle both sync and async)
                            if hasattr(self.rate_limiter, "backoff"):
                                if asyncio.iscoroutinefunction(
                                    self.rate_limiter.backoff
                                ):
                                    await self.rate_limiter.backoff(response)
                                else:
                                    self.rate_limiter.backoff(response)

                            # Retry with incremented count
                            kwargs["_rate_limit_retry_count"] = (
                                rate_limit_retry_count + 1
                            )
                            return await self.request(method, url, **kwargs)  # Retry

                    # Check if we should retry for server errors (not 429)
                    if (
                        response.status_code in self.retry_status_codes
                        and attempt < self.max_retries
                    ):
                        backoff_time = self.backoff_factor**attempt
                        logger.warning(
                            f"Request {request_id}: Server error {response.status_code}, "
                            f"retrying in {backoff_time:.1f}s (attempt {attempt + 1}/{self.max_retries + 1})"
                        )
                        await asyncio.sleep(backoff_time)
                        continue  # Retry the request

                    # Log final result
                    if response.ok:
                        logger.debug(
                            f"Request {request_id}: Success ({response.status_code})"
                        )
                    else:
                        logger.warning(
                            f"Request {request_id}: HTTP error {response.status_code} - {response.reason}"
                        )

                    return response

                except httpx.TimeoutException as e:
                    if attempt < self.max_retries:
                        backoff_time = self.backoff_factor**attempt
                        logger.warning(
                            f"Request {request_id}: Timeout, retrying in {backoff_time:.1f}s (attempt {attempt + 1})"
                        )
                        await asyncio.sleep(backoff_time)
                        continue
                    else:
                        logger.error(
                            f"Request {request_id}: Timeout after {self.max_retries + 1} attempts - {method} {url}"
                        )
                        return None

                except httpx.ConnectError as e:
                    if attempt < self.max_retries:
                        backoff_time = self.backoff_factor**attempt
                        logger.warning(
                            f"Request {request_id}: Connection error, retrying in {backoff_time:.1f}s (attempt {attempt + 1})"
                        )
                        await asyncio.sleep(backoff_time)
                        continue
                    else:
                        logger.error(
                            f"Request {request_id}: Connection error after {self.max_retries + 1} attempts - {method} {url}"
                        )
                        return None

            # All retries exhausted for server errors
            logger.error(f"Request {request_id}: All retry attempts exhausted")
            return None

        except httpx.HTTPError as e:
            logger.error(f"Request {request_id}: HTTP error - {method} {url} - {e}")
            return None
        except Exception as e:
            logger.error(
                f"Request {request_id}: Unexpected error - {method} {url} - {e}"
            )
            return None
        finally:
            # IMPORTANT: The rate limiter's semaphore is NOT automatically released here
            # to allow the caller to control when it's safe to make the next request.
            # Only release if rate limiter has a release method AND it's not acquired
            # (some rate limiters manage their own release timing)
            pass

    def _log_request(
        self, request_id: int, method: str, url: str, params: dict, headers: dict
    ):
        """Log request details with sensitive data sanitization."""
        # Sanitize sensitive parameters and headers
        safe_params = {}
        if params:
            safe_params = {
                k: v
                for k, v in params.items()
                if not any(
                    sensitive in k.lower()
                    for sensitive in ["token", "key", "secret", "password"]
                )
            }

        safe_headers = {}
        if headers:
            safe_headers = {
                k: (
                    "***REDACTED***"
                    if any(
                        sensitive in k.lower()
                        for sensitive in ["token", "key", "secret", "auth"]
                    )
                    else v
                )
                for k, v in headers.items()
            }

        logger.debug(f"[Async] Request {request_id}: {method.upper()} {url}")
        if safe_params:
            logger.debug(f"[Async] Request {request_id}: Params {safe_params}")
        if safe_headers:
            logger.debug(f"[Async] Request {request_id}: Headers {safe_headers}")

    def _log_response(
        self,
        request_id: int,
        method: str,
        url: str,
        response: httpx.Response,
        duration: float,
        attempt: int = 0,
    ):
        """Log response details."""
        attempt_info = f" (attempt {attempt + 1})" if attempt > 0 else ""
        logger.debug(
            f"[Async] Request {request_id}: {method.upper()} {url} -> {response.status_code} in {duration:.2f}s{attempt_info}"
        )

        # Log rate limit headers if present (for debugging)
        if logger.isEnabledFor(logging.DEBUG):
            rate_headers = {
                k: v
                for k, v in response.headers.items()
                if any(
                    rate_key in k.lower()
                    for rate_key in ["rate", "limit", "remaining", "reset"]
                )
            }
            if rate_headers:
                logger.debug(
                    f"[Async] Request {request_id}: Rate limit headers {rate_headers}"
                )

    async def close(self):
        """Close the underlying async HTTP client to free memory."""
        if self._client:
            await self._client.aclose()
            self._client = None

        # Cancel restore task if running
        if self._restore_task and not self._restore_task.done():
            self._restore_task.cancel()
            try:
                await self._restore_task
            except asyncio.CancelledError:
                pass

    def get_stats(self):
        """Get client statistics."""
        return {
            "total_requests": self._request_count,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_status_codes": list(self.retry_status_codes),
        }

    async def __aenter__(self):
        # Don't create client here, let it be created on first request
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# -----------------------------
# Utility Functions
# -----------------------------


def create_sync_client(
    rate_limiter: Optional[BaseRateLimiter] = None, **kwargs
) -> SyncHttpClient:
    """
    Factory function to create a configured sync HTTP client.

    Args:
        rate_limiter: Optional rate limiter instance
        **kwargs: Additional configuration options

    Returns:
        Configured SyncHttpClient instance
    """
    return SyncHttpClient(rate_limiter=rate_limiter, **kwargs)


def create_async_client(
    rate_limiter: Optional[BaseRateLimiter] = None, **kwargs
) -> AsyncHttpClient:
    """
    Factory function to create a configured async HTTP client.

    Args:
        rate_limiter: Optional rate limiter instance
        **kwargs: Additional configuration options

    Returns:
        Configured AsyncHttpClient instance
    """
    return AsyncHttpClient(rate_limiter=rate_limiter, **kwargs)
