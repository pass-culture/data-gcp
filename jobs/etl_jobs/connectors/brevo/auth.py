from typing import Tuple

from http_tools.auth import BaseAuthManager


class BrevoAuthManager(BaseAuthManager):
    """Handles Brevo static API key authentication."""

    def __init__(self, api_key: str):
        # API Keys are static, so we set refresh_buffer to None
        super().__init__(refresh_buffer=None)
        self.api_key = api_key

    def _fetch_token_data(self) -> Tuple[str, int]:
        # Return the key and a long expiry (1 year)
        return self.api_key, 31536000

    def get_headers(self, token: str) -> dict:
        """Override default Bearer header for Brevo's specific requirement."""
        return {
            "api-key": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
