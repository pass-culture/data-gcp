"""Titelive API client using http_tools."""

from typing import Any

import requests
from http_tools.clients import SyncHttpClient
from http_tools.rate_limiters import SyncTokenBucketRateLimiter

from src.api.auth import TokenManager
from src.constants import (
    EAN_SEPARATOR,
    RESPONSE_ENCODING,
    TITELIVE_BASE_URL,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


class TiteliveClient:
    """Client for interacting with Titelive API."""

    def __init__(
        self,
        token_manager: TokenManager,
        rate_limit_calls: int = 1,
        rate_limit_period: int = 1,
    ):
        """
        Initialize Titelive API client.

        Args:
            token_manager: Token manager for authentication
            rate_limit_calls: Number of calls allowed per period
            rate_limit_period: Time period in seconds for rate limiting
        """
        self.token_manager = token_manager
        self.rate_limiter = SyncTokenBucketRateLimiter(
            calls=rate_limit_calls,
            period=rate_limit_period,
        )
        self.http_client = SyncHttpClient(rate_limiter=self.rate_limiter)

    def _get_headers(self) -> dict[str, str]:
        """
        Get request headers with current authentication token.

        Returns:
            Headers dict with authorization
        """
        token = self.token_manager.get_token()
        return {"Authorization": f"Bearer {token}"}

    def _make_request(
        self, method: str, url: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Make an HTTP request with token refresh handling.

        Args:
            method: HTTP method
            url: Request URL
            params: Query parameters

        Returns:
            Parsed JSON response

        Raises:
            requests.exceptions.RequestException: If request fails after retry
        """
        headers = self._get_headers()

        response = self.http_client.request(method, url, headers=headers, params=params)

        if response is None:
            msg = f"Request failed for {url}"
            raise requests.exceptions.RequestException(msg)

        # Handle token expiration
        if response.status_code == 401:
            logger.warning("Token expired, refreshing and retrying")
            self.token_manager.refresh_token()
            headers = self._get_headers()
            response = self.http_client.request(
                method, url, headers=headers, params=params
            )

            if response is None:
                msg = f"Request failed after token refresh for {url}"
                raise requests.exceptions.RequestException(msg)

        response.encoding = RESPONSE_ENCODING
        return response.json()

    def get_by_eans(self, ean_list: list[str]) -> dict[str, Any]:
        """
        Fetch product data by EAN codes.

        Args:
            ean_list: List of EAN codes

        Returns:
            API response as dictionary

        Raises:
            ValueError: If ean_list is empty
            requests.exceptions.RequestException: If request fails
        """
        if not ean_list:
            msg = "EAN list cannot be empty"
            raise ValueError(msg)

        ean_param = EAN_SEPARATOR.join(ean_list)
        url = f"{TITELIVE_BASE_URL}/ean"
        params = {"in": f"ean={ean_param}"}

        logger.info(f"Fetching {len(ean_list)} products by EAN")
        return self._make_request("GET", url, params=params)

    def search_by_date(
        self,
        base: str,
        min_date: str,
        max_date: str | None = None,
        page: int = 1,
        results_per_page: int = 120,
    ) -> dict[str, Any]:
        """
        Search products by modification date range.

        Args:
            base: Product category (e.g., "paper", "music")
            min_date: Minimum modification date (DD/MM/YYYY format)
            max_date: Maximum modification date (DD/MM/YYYY format), optional
            page: Page number (starts at 1)
            results_per_page: Number of results per page

        Returns:
            API response as dictionary with 'result' and metadata

        Raises:
            requests.exceptions.RequestException: If request fails
        """
        url = f"{TITELIVE_BASE_URL}/search"
        params = {
            "base": base,
            "dateminm": min_date,
            "nombre": str(results_per_page),
            "page": str(page),
        }

        if max_date:
            params["datemaxm"] = max_date

        logger.info(
            f"Searching products: base={base}, min_date={min_date}, "
            f"max_date={max_date}, page={page}, per_page={results_per_page}"
        )
        return self._make_request("GET", url, params=params)
