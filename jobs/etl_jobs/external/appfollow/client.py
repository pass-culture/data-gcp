from typing import Any, Dict, List

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class AppFollowAPIError(Exception):
    """Custom exception for AppFollow API errors."""

    pass


class AppFollowClient:
    """
    API client to connect with AppFollow V2 services

    This class handles authentication, API requests, and error handling
    for AppFollow API endpoints.
    """

    BASE_API_URL = "https://api.appfollow.io/api/v2"

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""

        session = requests.Session()
        session.headers.update(
            {
                "accept": "application/json",
                "X-AppFollow-API-Token": self.api_token,
            }
        )

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        return session

    def get_reviews_page(
        self, ext_id: str, from_date: str, to_date: str, page: int
    ) -> Dict[str, Any]:
        """
        Get reviews of a specific page (maximum 100 reviews) using AppFollow API

        Args:
            ext_id: App external ID
            from_date: Start date
            to_date: End date
            page: Page

        Returns:
            dict: API response containing reviews data
        """

        url = f"{self.BASE_API_URL}/reviews"
        params = {
            "ext_id": ext_id,
            "from": from_date,
            "to": to_date,
            "page": page,
        }
        response = self.session.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            raise AppFollowAPIError(
                f"Reviews fetch failed: {response.status_code} - {response.text}"
            )

    def get_all_reviews(
        self, ext_id: str, from_date: str, to_date: str
    ) -> List[Dict[str, Any]]:
        """
        Extract all reviews for a specific app within a date range.

        Args:
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns: List of raw reviews data
        """
        has_more = True
        all_reviews = []
        page = 1

        try:
            while has_more:
                results = self.get_reviews_page(
                    ext_id=ext_id,
                    from_date=from_date,
                    to_date=to_date,
                    page=page,
                )
                reviews_data = results.get("reviews", {})
                reviews = reviews_data.get("list", [])

                all_reviews.extend(reviews)
                logger.debug(f"Imported page {page} with {len(reviews)} reviews")

                next_page = reviews_data.get("page", {}).get("next")

                if next_page:
                    page = next_page
                else:
                    has_more = False

        except AppFollowAPIError as e:
            logger.error(
                f"Erreur fetching reviews between {from_date} and {to_date}, page {page}: {e}"
            )
            raise

        return all_reviews
