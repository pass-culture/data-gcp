"""Authentication and token management for Titelive API."""

import requests

from src.constants import TITELIVE_TOKEN_ENDPOINT
from src.utils.gcp import access_secret_data
from src.utils.logging import get_logger

logger = get_logger(__name__)


class TokenManager:
    """Manages Titelive API authentication tokens."""

    def __init__(self, project_id: str):
        """
        Initialize token manager.

        Args:
            project_id: GCP project ID for accessing secrets
        """
        self.project_id = project_id
        self.username = access_secret_data(project_id, "titelive_epagine_api_username")
        self.password = access_secret_data(project_id, "titelive_epagine_api_password")
        self._token = None

    def get_token(self) -> str:
        """
        Get a valid token, fetching if necessary.

        Returns:
            Valid authentication token

        Raises:
            ValueError: If token cannot be obtained
            requests.exceptions.RequestException: If API request fails
        """
        if self._token is None:
            self._token = self._fetch_token()
        return self._token

    def refresh_token(self) -> str:
        """
        Force refresh the authentication token.

        Returns:
            New authentication token

        Raises:
            ValueError: If token cannot be obtained
            requests.exceptions.RequestException: If API request fails
        """
        logger.info("Refreshing Titelive authentication token")
        self._token = self._fetch_token()
        return self._token

    def _fetch_token(self) -> str:
        """
        Fetch a new token from Titelive API.

        Returns:
            Authentication token

        Raises:
            ValueError: If required credentials are missing or token not in response
            requests.exceptions.RequestException: If API request fails
        """
        if not all([self.username, self.password]):
            msg = "Missing required Titelive credentials"
            raise ValueError(msg)

        url = f"{TITELIVE_TOKEN_ENDPOINT}/{self.username}/token"
        headers = {"Content-Type": "application/json"}
        body = {"password": self.password}

        try:
            response = requests.post(url, headers=headers, json=body, timeout=30)
            response.raise_for_status()

            response_data = response.json()
            token = response_data.get("token")

            if not token:
                msg = "No token found in response"
                raise ValueError(msg)

            logger.info("Successfully obtained Titelive authentication token")
            return token

        except requests.exceptions.RequestException as e:
            logger.error(f"Error obtaining Titelive token: {e}")
            raise
