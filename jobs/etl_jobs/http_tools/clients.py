import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

import httpx
import requests
from http_tools.auth import BaseAuthManager
from http_tools.rate_limiters import BaseRateLimiter

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
    ):
        self.rate_limiter = rate_limiter
        self.auth_manager = auth_manager

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
    ):
        super().__init__(rate_limiter, auth_manager)
        self.max_retries = max_retries

    def request(
        self,
        method: str,
        url: str,
        skip_rate_limit: bool = False,
        _retry_count: int = 0,
        _auth_retry_count: int = 0,  # Added to prevent infinite loops on bad creds
        **kwargs,
    ):
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
    ):
        super().__init__(rate_limiter, auth_manager)
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
