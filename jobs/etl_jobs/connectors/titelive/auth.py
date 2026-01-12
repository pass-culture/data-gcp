"""Authentication manager for Titelive API."""

import logging
import threading
import time

import requests
from connectors.titelive.config import (
    TITELIVE_TOKEN_ENDPOINT,
    TOKEN_LIFETIME_SECONDS,
    TOKEN_REFRESH_BUFFER_SECONDS,
    get_titelive_credentials,
)
from http_tools.auth import BaseAuthManager

logger = logging.getLogger(__name__)


class TiteliveAuthManager(BaseAuthManager):
    """
    Titelive authentication manager with auto-refresh.

    Features:
    - Auto-refresh token at 4.5 minutes (before 5-minute expiry)
    - Thread-safe token access
    - Fetches credentials from GCP Secret Manager
    """

    def __init__(self, project_id: str):
        """
        Initialize Titelive auth manager.

        Args:
            project_id: GCP project ID for accessing secrets
        """
        self.project_id = project_id
        self.username, self.password = get_titelive_credentials(project_id)
        self._token = None
        self._token_timestamp = None
        self._lock = threading.Lock()
        self.token_lifetime = TOKEN_LIFETIME_SECONDS
        self.refresh_buffer = TOKEN_REFRESH_BUFFER_SECONDS

    def get_token(self, force: bool = False) -> str:
        """
        Get a valid token, auto-refreshing if needed.

        Args:
            force: Force token refresh

        Returns:
            Valid authentication token
        """
        with self._lock:
            if force or self._should_refresh():
                self._fetch_and_store_token()
            return self._token

    async def get_atoken(self, force: bool = False) -> str:
        """Async version (Titelive doesn't need async auth)."""
        return self.get_token(force=force)

    def get_headers(self, token: str) -> dict:
        """
        Build authorization headers.

        Args:
            token: Authentication token

        Returns:
            Headers dict with authorization
        """
        return {"Authorization": f"Bearer {token}"}

    def _should_refresh(self) -> bool:
        """Check if token needs refresh."""
        if self._token is None or self._token_timestamp is None:
            return True

        elapsed = time.time() - self._token_timestamp
        time_until_expiry = self.token_lifetime - elapsed

        # Refresh if within buffer period of expiry
        return time_until_expiry <= self.refresh_buffer

    def _fetch_and_store_token(self):
        """Fetch new token and update timestamp."""
        logger.info("🔄 Fetching Titelive authentication token")

        url = f"{TITELIVE_TOKEN_ENDPOINT}/{self.username}/token"
        headers = {"Content-Type": "application/json"}
        body = {"password": self.password}

        try:
            response = requests.post(url, headers=headers, json=body, timeout=30)
            response.raise_for_status()

            response_data = response.json()
            token = response_data.get("token")

            if not token:
                raise ValueError("No token found in response")

            self._token = token
            self._token_timestamp = time.time()

            logger.info(
                f"✅ Titelive token obtained "
                f"(will auto-refresh in {self.token_lifetime - self.refresh_buffer}s)"
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to fetch Titelive token: {e}")
            raise
