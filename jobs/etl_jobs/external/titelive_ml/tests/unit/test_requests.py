"""Tests for the requests utility functions."""

from unittest.mock import Mock, patch

import pytest
import requests


class TestTiteliveTokenWithMocking:
    """Test cases for Titelive token management with full mocking."""

    @patch("src.constants.TITELIVE_USERNAME", "test_user")
    @patch("src.constants.TITELIVE_PASSWORD", "test_pass")
    @patch("src.constants.TITELIVE_TOKEN_ENDPOINT", "https://test.api.com/login")
    @patch("src.utils.requests.requests.post")
    def test_get_titelive_token_success(self, mock_post):
        """Test successful token retrieval."""
        from src.utils.requests import get_titelive_token

        # Arrange
        mock_response = Mock()
        mock_response.json.return_value = {"token": "test_token_123"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Act
        token = get_titelive_token()

        # Assert
        assert token == "test_token_123"
        mock_post.assert_called_once()
        mock_response.raise_for_status.assert_called_once()

    @patch("src.constants.TITELIVE_USERNAME", "test_user")
    @patch("src.constants.TITELIVE_PASSWORD", "test_pass")
    @patch("src.constants.TITELIVE_TOKEN_ENDPOINT", "https://test.api.com/login")
    @patch("src.utils.requests.requests.post")
    def test_get_titelive_token_no_token_in_response(self, mock_post):
        """Test handling when no token is returned."""
        from src.utils.requests import get_titelive_token

        # Arrange
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        # Act & Assert
        with pytest.raises(ValueError, match="No token found in response"):
            get_titelive_token()

    @patch("src.constants.TITELIVE_USERNAME", "test_user")
    @patch("src.constants.TITELIVE_PASSWORD", "test_pass")
    @patch("src.constants.TITELIVE_TOKEN_ENDPOINT", "https://test.api.com/login")
    @patch("src.utils.requests.requests.post")
    def test_get_titelive_token_request_exception(self, mock_post):
        """Test handling of request exceptions."""
        from src.utils.requests import get_titelive_token

        # Arrange
        mock_post.side_effect = requests.exceptions.RequestException("Network error")

        # Act & Assert
        with pytest.raises(requests.exceptions.RequestException):
            get_titelive_token()


class TestFetchGetRequest:
    """Test cases for fetch_get_request function."""

    @patch("src.utils.requests.requests.get")
    @patch("src.utils.requests._refresh_token")
    def test_fetch_get_request_success(self, mock_refresh_token, mock_get):
        """Test successful GET request."""
        from src.utils.requests import fetch_get_request

        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.encoding = "utf-8"
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        url = "https://test.com/api"
        headers = {"Authorization": "Bearer token"}
        params = {"param1": "value1"}

        # Act
        result = fetch_get_request(url, headers, params)

        # Assert
        assert result == {"success": True}
        mock_get.assert_called_once_with(
            url, headers=headers, params=params, timeout=30
        )
        assert mock_response.encoding == "utf-8"

    @patch("src.utils.requests.requests.get")
    @patch("src.utils.requests._refresh_token")
    def test_fetch_get_request_token_refresh_on_401(self, mock_refresh_token, mock_get):
        """Test token refresh on 401 error."""
        from src.utils.requests import fetch_get_request

        # Arrange
        # First call returns 401, second call succeeds
        failed_response = Mock()
        failed_response.status_code = 401

        success_response = Mock()
        success_response.status_code = 200
        success_response.encoding = "utf-8"
        success_response.json.return_value = {"success": True}
        success_response.raise_for_status.return_value = None

        mock_get.side_effect = [failed_response, success_response]
        mock_refresh_token.return_value = "new_token"

        url = "https://test.com/api"
        headers = {"Authorization": "Bearer old_token"}

        # Act
        result = fetch_get_request(url, headers)

        # Assert
        assert result == {"success": True}
        assert mock_get.call_count == 2
        mock_refresh_token.assert_called_once()
        # Check that header was updated with new token
        assert headers["Authorization"] == "Bearer new_token"

    @patch("src.utils.requests.requests.get")
    def test_fetch_get_request_exception_handling(self, mock_get):
        """Test exception handling in fetch_get_request."""
        from src.utils.requests import fetch_get_request

        # Arrange
        mock_get.side_effect = requests.exceptions.RequestException("Network error")

        # Act & Assert
        with pytest.raises(requests.exceptions.RequestException):
            fetch_get_request("https://test.com/api", {})
