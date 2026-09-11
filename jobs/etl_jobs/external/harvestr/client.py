from typing import Any, Dict, List

import requests
from harvestr.utils import SecretStr
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class HarvestrAPIError(Exception):
    """Custom exception for Harvestr API errors."""

    pass


class HarvestrClient:
    """
    API client to connect with Harvestr services

    This class handles authentication, API requests, and error handling
    for Harvestr API endpoints.
    """

    BASE_API_URL = "https://rest.harvestr.io/v1"

    def __init__(self, api_token: SecretStr):
        self.api_token = api_token
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""

        session = requests.Session()
        session.headers.update(
            {
                "accept": "application/json",
                "X-Harvestr-Private-App-Token": self.api_token.get_secret_value(),
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

    def get_all_messages(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """
        Extract all raw feedback messages within a date range.

        Args:
            from_date: Start date in YYYY-MM-DD format (inclusive lower bound)
            to_date: End date in YYYY-MM-DD format (exclusive upper bound)

        Returns: List of raw message data
        """
        all_messages = []
        offset = 0
        per_page = 100

        while True:
            url = f"{self.BASE_API_URL}/message"
            params = {
                "per_page": per_page,
                "offset": offset,
                "created_after": from_date,
                "created_before": to_date,
            }

            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                messages = data.get("messages", [])

                if not messages:
                    break

                all_messages.extend(messages)

                if len(messages) < per_page:
                    break

                offset += per_page

            except requests.exceptions.RequestException as e:
                raise HarvestrAPIError(f"Failed to fetch messages: {e}")

        return all_messages
