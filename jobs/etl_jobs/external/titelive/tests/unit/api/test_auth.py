"""Tests for Titelive API authentication and token management."""

from unittest.mock import Mock, patch

import pytest
import requests

from src.api.auth import TokenManager


class TestTokenManager:
    """Test cases for TokenManager class."""

    def test_init_fetches_credentials_from_gcp(self):
        """Test that TokenManager initializes and fetches credentials from GCP."""
        # Arrange & Act
        with patch("src.api.auth.access_secret_data") as mock_access_secret:
            mock_access_secret.side_effect = lambda project_id, secret_id: {
                "titelive_epagine_api_username": "test_user",
                "titelive_epagine_api_password": "test_pass",
            }[secret_id]

            manager = TokenManager(project_id="test-project")

        # Assert
        assert manager.project_id == "test-project"
        assert manager.username == "test_user"
        assert manager.password == "test_pass"
        assert manager._token is None

    def test_get_token_fetches_token_on_first_call(
        self, mock_token_manager_with_secrets
    ):
        """Test that get_token fetches a new token on first call."""
        # Arrange
        manager = mock_token_manager_with_secrets
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "new_token_123"}

        # Act
        with patch("requests.post") as mock_post:
            mock_post.return_value = mock_response
            token = manager.get_token()

        # Assert
        assert token == "new_token_123"
        assert manager._token == "new_token_123"
        mock_post.assert_called_once()

    def test_get_token_returns_cached_token(self, mock_token_manager_with_secrets):
        """Test that get_token returns cached token on subsequent calls."""
        # Arrange
        manager = mock_token_manager_with_secrets
        manager._token = "cached_token_456"

        # Act
        with patch("requests.post") as mock_post:
            token = manager.get_token()

        # Assert
        assert token == "cached_token_456"
        mock_post.assert_not_called()

    def test_refresh_token_forces_new_fetch(self, mock_token_manager_with_secrets):
        """Test that refresh_token forces a new token fetch."""
        # Arrange
        manager = mock_token_manager_with_secrets
        manager._token = "old_token"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "refreshed_token_789"}

        # Act
        with patch("requests.post") as mock_post:
            mock_post.return_value = mock_response
            token = manager.refresh_token()

        # Assert
        assert token == "refreshed_token_789"
        assert manager._token == "refreshed_token_789"
        mock_post.assert_called_once()

    def test_fetch_token_successful(self, mock_token_manager_with_secrets):
        """Test successful token fetch from API."""
        # Arrange
        manager = mock_token_manager_with_secrets
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"token": "valid_token_xyz"}

        # Act
        with patch("requests.post") as mock_post:
            mock_post.return_value = mock_response
            token = manager._fetch_token()

        # Assert
        assert token == "valid_token_xyz"
        mock_post.assert_called_once_with(
            "https://login.epagine.fr/v1/login/test_username/token",
            headers={"Content-Type": "application/json"},
            json={"password": "test_password"},
            timeout=30,
        )

    def test_fetch_token_missing_credentials(self):
        """Test error handling when credentials are missing."""
        # Arrange
        with patch("src.api.auth.access_secret_data") as mock_access_secret:
            mock_access_secret.return_value = None
            manager = TokenManager(project_id="test-project")

        # Act & Assert
        with pytest.raises(ValueError, match="Missing required Titelive credentials"):
            manager._fetch_token()

    def test_fetch_token_no_token_in_response(self, mock_token_manager_with_secrets):
        """Test error handling when token is missing from API response."""
        # Arrange
        manager = mock_token_manager_with_secrets
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "success"}  # Missing 'token' key

        # Act & Assert
        with patch("requests.post") as mock_post:
            mock_post.return_value = mock_response
            with pytest.raises(ValueError, match="No token found in response"):
                manager._fetch_token()

    def test_fetch_token_http_error(self, mock_token_manager_with_secrets):
        """Test error handling for HTTP errors during token fetch."""
        # Arrange
        manager = mock_token_manager_with_secrets
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "401 Unauthorized"
        )

        # Act & Assert
        with patch("requests.post") as mock_post:
            mock_post.return_value = mock_response
            with pytest.raises(requests.exceptions.HTTPError):
                manager._fetch_token()

    def test_fetch_token_network_error(self, mock_token_manager_with_secrets):
        """Test error handling for network errors during token fetch."""
        # Arrange
        manager = mock_token_manager_with_secrets

        # Act & Assert
        with patch("requests.post") as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError("Network error")
            with pytest.raises(requests.exceptions.ConnectionError):
                manager._fetch_token()

    def test_fetch_token_timeout_error(self, mock_token_manager_with_secrets):
        """Test error handling for timeout errors during token fetch."""
        # Arrange
        manager = mock_token_manager_with_secrets

        # Act & Assert
        with patch("requests.post") as mock_post:
            mock_post.side_effect = requests.exceptions.Timeout("Request timeout")
            with pytest.raises(requests.exceptions.Timeout):
                manager._fetch_token()
