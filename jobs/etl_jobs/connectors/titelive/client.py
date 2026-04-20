"""Titelive API connector using http_tools."""

import logging
from typing import Any

from connectors.titelive.config import (
    EAN_SEPARATOR,
    MAX_EANS_PER_REQUEST,
    RESPONSE_ENCODING,
    TITELIVE_BASE_URL,
)
from http_tools.clients import SyncHttpClient

logger = logging.getLogger(__name__)


class TiteliveConnector:
    """
    Titelive API connector.

    Implements API routes using injected SyncHttpClient.
    """

    BASE_URL = TITELIVE_BASE_URL

    def __init__(self, client: SyncHttpClient):
        """
        Initialize Titelive connector.

        Args:
            client: Configured SyncHttpClient with auth, rate limiting, etc.
        """
        self.client = client

    def get_by_eans(self, ean_list: list[str]) -> dict[str, Any]:
        """
        Fetch product data by EAN codes.

        Args:
            ean_list: List of EAN codes (max 250)

        Returns:
            API response as dictionary

        Raises:
            ValueError: If ean_list is empty or exceeds 250
            HttpClientError: If request fails
        """
        if not ean_list:
            raise ValueError("EAN list cannot be empty")

        if len(ean_list) > MAX_EANS_PER_REQUEST:
            raise ValueError(
                f"EAN list exceeds API limit ({MAX_EANS_PER_REQUEST}): {len(ean_list)}"
            )

        ean_param = EAN_SEPARATOR.join(ean_list)
        url = f"{self.BASE_URL}/ean"
        params = {"in": f"ean={ean_param}"}

        logger.debug(f"📦 Fetching {len(ean_list)} EANs")

        response = self.client.request("GET", url, params=params)

        # Set encoding for response
        if hasattr(response, "encoding"):
            response.encoding = RESPONSE_ENCODING

        return response.json()

    def get_by_eans_with_base(self, ean_list: list[str], base: str) -> dict[str, Any]:
        """
        Fetch product data by EAN codes with specific base category.

        Args:
            ean_list: List of EAN codes (max 250)
            base: Product category (e.g., "paper", "music")

        Returns:
            API response as dictionary

        Raises:
            ValueError: If ean_list is empty or exceeds 250
            HttpClientError: If request fails
        """
        if not ean_list:
            raise ValueError("EAN list cannot be empty")

        if len(ean_list) > MAX_EANS_PER_REQUEST:
            raise ValueError(
                f"EAN list exceeds API limit ({MAX_EANS_PER_REQUEST}): {len(ean_list)}"
            )

        ean_param = EAN_SEPARATOR.join(ean_list)
        url = f"{self.BASE_URL}/ean"
        params = {"in": f"ean={ean_param}", "base": base}

        logger.debug(f"📦 Fetching {len(ean_list)} EANs (base={base})")

        response = self.client.request("GET", url, params=params)

        if hasattr(response, "encoding"):
            response.encoding = RESPONSE_ENCODING

        return response.json()

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
            results_per_page: Number of results per page (max 120)

        Returns:
            API response as dictionary with 'result' and metadata

        Raises:
            HttpClientError: If request fails
        """
        url = f"{self.BASE_URL}/search"
        params = {
            "base": base,
            "dateminm": min_date,
            "nombre": str(results_per_page),
            "page": str(page),
        }

        if max_date:
            params["datemaxm"] = max_date

        logger.debug(
            f"🔍 Searching products (base={base}, date={min_date}, page={page})"
        )

        response = self.client.request("GET", url, params=params)

        if hasattr(response, "encoding"):
            response.encoding = RESPONSE_ENCODING

        return response.json()
