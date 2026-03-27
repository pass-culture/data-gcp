import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Optional

import httpx
import requests
from http_tools.auth import BaseAuthManager
from http_tools.circuit_breakers import CircuitBreaker, CircuitBreakerOpenError
from http_tools.exceptions import (
    AuthenticationError,
    NetworkError,
    RateLimitError,
    classify_http_error,
    parse_retry_after,
)
from http_tools.rate_limiters import BaseRateLimiter
from http_tools.retry_strategies import BaseRetryStrategy

ENV_LOG_LEVEL = os.getenv("CLIENT_LOG_LEVEL", "INFO").upper()
CLIENT_LOG_LEVEL = getattr(logging, ENV_LOG_LEVEL, logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(CLIENT_LOG_LEVEL)


# -----------------------------
# Base HTTP Client (abstract)
# -----------------------------
class BaseHttpClient(ABC):
    def __init__(
        self,
        rate_limiter: Optional[BaseRateLimiter] = None,
        auth_manager: Optional[BaseAuthManager] = None,
        retry_strategy: Optional[BaseRetryStrategy] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        use_legacy_retry: bool = True,  # Backwards compatibility flag
    ):
        self.rate_limiter = rate_limiter
        self.auth_manager = auth_manager
        self.retry_strategy = retry_strategy
        self.circuit_breaker = circuit_breaker
        self.use_legacy_retry = use_legacy_retry

        # If retry_strategy provided, disable legacy retry
        if retry_strategy is not None:
            self.use_legacy_retry = False

    def set_rate_limiter(self, limiter: BaseRateLimiter):
        """Allow switching limiters at runtime (useful for dynamic APIs)."""
        self.rate_limiter = limiter

    def set_auth_manager(self, manager: BaseAuthManager):
        """Allow switching auth strategies at runtime."""
        self.auth_manager = manager

    # --- Standardized Logging Helpers ---
    def _log_request(self, method: str, url: str):
        """Log the start of an HTTP request."""
        logger.debug(f"🌐 [{self.__class__.__name__}] {method} {url}")

    def _log_retry(self, code: int, url: str, attempt: int, max_retries: int):
        """Log a retry event for 429 or 401."""
        icon = "⚠️" if code == 429 else "🔑"
        msg = "Rate Limit" if code == 429 else "Unauthorized"
        logger.warning(
            f"{icon} [{self.__class__.__name__}] {msg} hit. "
            f"Retry {attempt}/{max_retries} for {url}"
        )

    def _log_retry_strategy(self, attempt: int, max_retries: int, wait: float):
        """Log a retry event using new retry strategy."""
        logger.warning(
            f"⚠️ [{self.__class__.__name__}] Retry {attempt}/{max_retries}, "
            f"waiting {wait:.2f}s"
        )

    def _log_error(self, msg: str, url: str, code: Optional[int] = None):
        """Log a terminal error."""
        prefix = f"HTTP {code}" if code else "Error"
        logger.error(f"🛑 [{self.__class__.__name__}] {prefix}: {msg} | URL: {url}")

    @abstractmethod
    def request(self, method: str, url: str, **kwargs):
        """Perform a request with the given HTTP method and URL."""
        pass


# -----------------------------
# Sync HTTP Client
# -----------------------------
class SyncHttpClient(BaseHttpClient):
    def __init__(
        self,
        rate_limiter: Optional[BaseRateLimiter] = None,
        auth_manager: Optional[BaseAuthManager] = None,
        max_retries: int = 3,
        retry_strategy: Optional[BaseRetryStrategy] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        use_legacy_retry: bool = True,
    ):
        super().__init__(
            rate_limiter,
            auth_manager,
            retry_strategy,
            circuit_breaker,
            use_legacy_retry,
        )
        self.max_retries = max_retries

    def request(
        self,
        method: str,
        url: str,
        skip_rate_limit: bool = False,
        _retry_count: int = 0,
        _auth_retry_count: int = 0,
        **kwargs,
    ):
        # Use new strategy-based approach if available
        if not self.use_legacy_retry and self.retry_strategy is not None:
            return self._request_with_strategy(method, url, **kwargs)

        # Otherwise use legacy retry logic (backwards compatible)
        return self._request_legacy(
            method, url, skip_rate_limit, _retry_count, _auth_retry_count, **kwargs
        )

    def _request_with_strategy(self, method: str, url: str, **kwargs):
        """New strategy-based request implementation."""

        # Check circuit breaker
        if self.circuit_breaker and not self.circuit_breaker.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker open for {url}")

        self._log_request(method, url)

        # Inject auth headers
        if self.auth_manager:
            token = self.auth_manager.get_token()
            auth_headers = self.auth_manager.get_headers(token)
            kwargs["headers"] = {**kwargs.get("headers", {}), **auth_headers}

        # Retry loop
        last_error = None
        while True:
            try:
                # Rate limiting
                if self.rate_limiter:
                    self.rate_limiter.acquire()

                # Make request
                response = requests.request(method, url, **kwargs)

                # Check for errors
                if response.status_code >= 400:
                    # Parse retry_after for 429 errors
                    retry_after = None
                    if response.status_code == 429:
                        retry_after = parse_retry_after(response)

                    error = classify_http_error(response.status_code, url)

                    # Set retry_after on RateLimitError
                    if isinstance(error, RateLimitError) and retry_after:
                        error.retry_after = retry_after

                    # Special handling for 429 (rate limit)
                    if isinstance(error, RateLimitError) and self.rate_limiter:
                        self.rate_limiter.backoff(response)

                    # Special handling for 401 (auth)
                    if isinstance(error, AuthenticationError) and self.auth_manager:
                        self.auth_manager.get_token(force=True)

                    raise error

                # Success!
                self.retry_strategy.reset()
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                return response

            except requests.RequestException as e:
                # Network error
                last_error = NetworkError(str(e), url, e)

            except Exception as e:
                last_error = e

            # Record failure
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()

            # Should we retry?
            if not self.retry_strategy.should_retry(last_error):
                self._log_error(str(last_error), url)
                raise last_error

            # Calculate backoff
            wait = self.retry_strategy.calculate_backoff(last_error)
            retry_count = self.retry_strategy.increment_retry_count()

            self._log_retry_strategy(
                retry_count, self.retry_strategy.policy.max_retries, wait
            )

            time.sleep(wait)

    def _request_legacy(
        self,
        method: str,
        url: str,
        skip_rate_limit: bool = False,
        _retry_count: int = 0,
        _auth_retry_count: int = 0,
        **kwargs,
    ):
        """Legacy retry logic for backwards compatibility."""

        # Log the start of the request
        if _retry_count == 0 and _auth_retry_count == 0:
            self._log_request(method, url)

        # --- Inject Auth Headers ---
        if self.auth_manager:
            token = self.auth_manager.get_token()
            auth_headers = self.auth_manager.get_headers(token)
            kwargs["headers"] = {**kwargs.get("headers", {}), **auth_headers}

        try:
            if self.rate_limiter and not skip_rate_limit:
                self.rate_limiter.acquire()

            response = requests.request(method, url, **kwargs)

            if response.status_code == 429:
                if _retry_count >= self.max_retries:
                    self._log_error("Max retries reached for 429", url, 429)
                    response.raise_for_status()

                if self.rate_limiter:
                    self._log_retry(429, url, _retry_count + 1, self.max_retries)
                    self.rate_limiter.backoff(response)
                    return self.request(
                        method,
                        url,
                        skip_rate_limit=True,
                        _retry_count=_retry_count + 1,
                        **kwargs,
                    )

            if (
                response.status_code == 401
                and self.auth_manager
                and _auth_retry_count < 1
            ):
                self._log_retry(401, url, 1, 1)
                self.auth_manager.get_token(force=True)
                return self.request(
                    method, url, _auth_retry_count=_auth_retry_count + 1, **kwargs
                )

            # Final check for 401 if refresh failed or was not allowed
            if response.status_code == 401:
                self._log_error(
                    "Unauthorized (Refresh failed or not configured)", url, 401
                )
                return response

            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            self._log_error(str(e), url, e.response.status_code)
            raise
        except requests.RequestException as e:
            self._log_error(f"Request failed: {str(e)}", url)
            return None


# -----------------------------
# Async HTTP Client
# -----------------------------
class AsyncHttpClient(BaseHttpClient):
    def __init__(
        self,
        rate_limiter: Optional[BaseRateLimiter] = None,
        auth_manager: Optional[BaseAuthManager] = None,
        retry_strategy: Optional[BaseRetryStrategy] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        use_legacy_retry: bool = True,
    ):
        super().__init__(
            rate_limiter,
            auth_manager,
            retry_strategy,
            circuit_breaker,
            use_legacy_retry,
        )
        self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            )
        return self._client

    async def request(
        self,
        method: str,
        url: str,
        _retry_count: int = 0,
        _auth_retry_count: int = 0,
        **kwargs,
    ):
        # Use new strategy-based approach if available
        if not self.use_legacy_retry and self.retry_strategy is not None:
            return await self._request_with_strategy(method, url, **kwargs)

        # Otherwise use legacy retry logic (backwards compatible)
        return await self._request_legacy(
            method, url, _retry_count, _auth_retry_count, **kwargs
        )

    async def _request_with_strategy(self, method: str, url: str, **kwargs):
        """New strategy-based async request implementation."""

        # Check circuit breaker
        if self.circuit_breaker and not self.circuit_breaker.allow_request():
            raise CircuitBreakerOpenError(f"Circuit breaker open for {url}")

        self._log_request(method, url)

        # Inject auth headers
        if self.auth_manager:
            token = await self.auth_manager.get_atoken()
            auth_headers = self.auth_manager.get_headers(token)
            kwargs["headers"] = {**kwargs.get("headers", {}), **auth_headers}

        # Retry loop
        last_error = None
        acquired = False

        while True:
            try:
                # Rate limiting
                if self.rate_limiter:
                    await self.rate_limiter.acquire()
                    acquired = True

                # Make request
                client = self._get_client()
                response = await client.request(method, url, **kwargs)

                # Check for errors
                if response.status_code >= 400:
                    # Release semaphore before backoff
                    if acquired and self.rate_limiter:
                        self.rate_limiter.release()
                        acquired = False

                    # Parse retry_after for 429 errors
                    retry_after = None
                    if response.status_code == 429:
                        retry_after = parse_retry_after(response)

                    error = classify_http_error(response.status_code, url)

                    # Set retry_after on RateLimitError
                    if isinstance(error, RateLimitError) and retry_after:
                        error.retry_after = retry_after

                    # Special handling for 429 (rate limit)
                    if isinstance(error, RateLimitError) and self.rate_limiter:
                        await self.rate_limiter.backoff(response)

                    # Special handling for 401 (auth)
                    if isinstance(error, AuthenticationError) and self.auth_manager:
                        await self.auth_manager.get_atoken(force=True)

                    raise error

                # Success!
                self.retry_strategy.reset()
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                return response

            except httpx.RequestError as e:
                last_error = NetworkError(str(e), url, e)

            except Exception as e:
                last_error = e

            finally:
                # Always release semaphore
                if acquired and self.rate_limiter:
                    self.rate_limiter.release()
                    acquired = False

            # Record failure
            if self.circuit_breaker:
                self.circuit_breaker.record_failure()

            # Should we retry?
            if not self.retry_strategy.should_retry(last_error):
                self._log_error(str(last_error), url)
                raise last_error

            # Calculate backoff
            wait = self.retry_strategy.calculate_backoff(last_error)
            retry_count = self.retry_strategy.increment_retry_count()

            self._log_retry_strategy(
                retry_count, self.retry_strategy.policy.max_retries, wait
            )

            await asyncio.sleep(wait)

    async def _request_legacy(
        self,
        method: str,
        url: str,
        _retry_count: int = 0,
        _auth_retry_count: int = 0,
        **kwargs,
    ):
        """Legacy retry logic for backwards compatibility."""

        # Log the start of the request
        if _retry_count == 0 and _auth_retry_count == 0:
            self._log_request(method, url)

        # --- Inject Auth Headers (Async) ---
        if self.auth_manager:
            token = await self.auth_manager.get_atoken()
            auth_headers = self.auth_manager.get_headers(token)
            kwargs["headers"] = {**kwargs.get("headers", {}), **auth_headers}

        acquired = False
        try:
            # 2. Proactive Throttling (Acquire)
            if self.rate_limiter:
                await self.rate_limiter.acquire()
                acquired = True

            client = self._get_client()
            response = await client.request(method, url, **kwargs)

            # 3. Handle 429 (Rate Limit)
            if response.status_code == 429 and self.rate_limiter:
                # Release semaphore before we sleep to let other tasks progress/queue
                if acquired and hasattr(self.rate_limiter, "release"):
                    self.rate_limiter.release()
                    acquired = False

                if _retry_count < 3:  # max_retries
                    self._log_retry(429, url, _retry_count + 1, 3)
                    await self.rate_limiter.backoff(response)
                    return await self.request(
                        method, url, _retry_count=_retry_count + 1, **kwargs
                    )
                else:
                    self._log_error("Max retries reached for 429", url, 429)
                    response.raise_for_status()

            # 4. Handle 401 (Auth Refresh)
            if (
                response.status_code == 401
                and self.auth_manager
                and _auth_retry_count < 1
            ):
                self._log_retry(401, url, 1, 1)

                if acquired and hasattr(self.rate_limiter, "release"):
                    self.rate_limiter.release()
                    acquired = False

                await self.auth_manager.get_atoken(force=True)
                return await self.request(
                    method,
                    url,
                    _auth_retry_count=_auth_retry_count + 1,
                    _retry_count=_retry_count,
                    **kwargs,
                )

            response.raise_for_status()
            return response

        except httpx.HTTPStatusError as e:
            self._log_error(str(e), url, e.response.status_code)
            raise
        except Exception as e:
            self._log_error(f"Unexpected error: {str(e)}", url)
            if acquired and self.rate_limiter and hasattr(self.rate_limiter, "release"):
                self.rate_limiter.release()
            raise
        finally:
            # Crucial: Always release if we still hold the semaphore
            if acquired and self.rate_limiter:
                self.rate_limiter.release()

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
