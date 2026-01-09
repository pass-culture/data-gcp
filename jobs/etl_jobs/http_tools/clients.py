import logging
from abc import ABC, abstractmethod
from typing import Optional

import httpx
import requests
from http_tools.auth import BaseAuthManager
from http_tools.rate_limiters import BaseRateLimiter

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
        # --- Inject Auth Headers ---
        if self.auth_manager:
            token = self.auth_manager.get_token()
            auth_headers = self.auth_manager.get_headers(token)
            kwargs["headers"] = {**kwargs.get("headers", {}), **auth_headers}

        try:
            # --- Rate Limiting ---
            if self.rate_limiter and not skip_rate_limit:
                self.rate_limiter.acquire()

            response = requests.request(method, url, **kwargs)

            # --- Handle 429 ---
            if response.status_code == 429:
                if _retry_count >= self.max_retries:
                    logger.error(f"Max retries ({self.max_retries}) reached for 429")
                    response.raise_for_status()
                    return response

                if self.rate_limiter:
                    self.rate_limiter.backoff(response)
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

            # --- Handle 401 ---
            if (
                response.status_code == 401
                and self.auth_manager
                and _auth_retry_count < 1
            ):
                logger.warning(
                    f"Received 401 for {url}. Refreshing token and retrying..."
                )
                self.auth_manager.get_token(force=True)
                return self.request(
                    method,
                    url,
                    _auth_retry_count=_auth_retry_count + 1,
                    _retry_count=_retry_count,
                    **kwargs,
                )

            # --- Final response handling ---
            if response.status_code == 401:
                logger.warning(
                    "Received 401 Unauthorized (Refresh failed or not configured)"
                )
                return response

            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            raise
        except requests.RequestException as e:
            logger.error(f"HTTP request failed: {e}")
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
        # --- Inject Auth Headers (Async) ---
        if self.auth_manager:
            token = await self.auth_manager.get_atoken()
            auth_headers = self.auth_manager.get_headers(token)
            kwargs["headers"] = {**kwargs.get("headers", {}), **auth_headers}

        acquired = False
        try:
            # --- Acquire Rate Limiter ---
            if self.rate_limiter:
                await self.rate_limiter.acquire()
                acquired = True

            client = self._get_client()
            response = await client.request(method, url, **kwargs)

            # --- Handle 429 ---
            if response.status_code == 429 and self.rate_limiter:
                if acquired and hasattr(self.rate_limiter, "release"):
                    self.rate_limiter.release()
                    acquired = False

                await self.rate_limiter.backoff(response)
                return await self.request(
                    method, url, _retry_count=_retry_count + 1, **kwargs
                )

            # --- Handle 401 ---
            if (
                response.status_code == 401
                and self.auth_manager
                and _auth_retry_count < 1
            ):
                logger.warning(f"Async 401 for {url}. Refreshing and retrying...")
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

        except httpx.HTTPError as e:
            logger.error(f"Async HTTP request failed: {e}")
            if acquired and self.rate_limiter and hasattr(self.rate_limiter, "release"):
                self.rate_limiter.release()
            return None
        except Exception as e:
            logger.error(f"Unexpected error in async request: {e}")
            if acquired and self.rate_limiter and hasattr(self.rate_limiter, "release"):
                self.rate_limiter.release()
            raise

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
