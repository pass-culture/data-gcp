"""Tests for Titelive API client."""

import sys
from unittest.mock import Mock

import pytest
import requests

# Mock http_tools before importing TiteliveClient
sys.modules["http_tools"] = Mock()
sys.modules["http_tools.clients"] = Mock()
sys.modules["http_tools.rate_limiters"] = Mock()

from src.api.client import TiteliveClient  # noqa: E402


class TestTiteliveClient:
    """Test cases for TiteliveClient class."""

    def test_init_creates_client_with_defaults(self, mock_token_manager):
        """Test that TiteliveClient initializes with default parameters."""
        # Act
        client = TiteliveClient(token_manager=mock_token_manager)

        # Assert
        assert client.token_manager == mock_token_manager
        assert client.max_token_refresh_retries == 2
        assert client.rate_limiter is not None
        assert client.http_client is not None

    def test_init_creates_client_with_custom_params(self, mock_token_manager):
        """Test that TiteliveClient initializes with custom parameters."""
        # Act
        client = TiteliveClient(
            token_manager=mock_token_manager,
            rate_limit_calls=5,
            rate_limit_period=2,
            max_token_refresh_retries=3,
            max_rate_limit_retries=5,
        )

        # Assert
        assert client.max_token_refresh_retries == 3

    def test_get_headers_returns_authorization_header(self, mock_token_manager):
        """Test that _get_headers returns correct authorization header."""
        # Arrange
        mock_token_manager.get_token.return_value = "test_token_123"
        client = TiteliveClient(token_manager=mock_token_manager)

        # Act
        headers = client._get_headers()

        # Assert
        assert headers == {"Authorization": "Bearer test_token_123"}
        mock_token_manager.get_token.assert_called_once()

    def test_get_by_eans_successful(self, mock_titelive_client, mock_api_response):
        """Test successful get_by_eans request."""
        # Arrange
        client = mock_titelive_client
        ean_list = ["9781234567890", "9789876543210"]

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.encoding = "utf-8"
        client.http_client.request.return_value = mock_response

        # Act
        result = client.get_by_eans(ean_list)

        # Assert
        assert result == mock_api_response
        client.http_client.request.assert_called_once_with(
            "GET",
            "https://catsearch.epagine.fr/v1/ean",
            headers={"Authorization": "Bearer test_token"},
            params={"in": "ean=9781234567890|9789876543210"},
        )

    def test_get_by_eans_empty_list_raises_error(self, mock_titelive_client):
        """Test that get_by_eans raises error for empty EAN list."""
        # Arrange
        client = mock_titelive_client

        # Act & Assert
        with pytest.raises(ValueError, match="EAN list cannot be empty"):
            client.get_by_eans([])

    def test_get_by_eans_with_base_successful(
        self, mock_titelive_client, mock_api_response
    ):
        """Test successful get_by_eans_with_base request."""
        # Arrange
        client = mock_titelive_client
        ean_list = ["9781234567890"]
        base = "paper"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.encoding = "utf-8"
        client.http_client.request.return_value = mock_response

        # Act
        result = client.get_by_eans_with_base(ean_list, base)

        # Assert
        assert result == mock_api_response
        client.http_client.request.assert_called_once_with(
            "GET",
            "https://catsearch.epagine.fr/v1/ean",
            headers={"Authorization": "Bearer test_token"},
            params={"in": "ean=9781234567890", "base": "paper"},
        )

    def test_get_by_eans_with_base_empty_list_raises_error(self, mock_titelive_client):
        """Test that get_by_eans_with_base raises error for empty EAN list."""
        # Arrange
        client = mock_titelive_client

        # Act & Assert
        with pytest.raises(ValueError, match="EAN list cannot be empty"):
            client.get_by_eans_with_base([], "paper")

    def test_search_by_date_successful(self, mock_titelive_client, mock_api_response):
        """Test successful search_by_date request."""
        # Arrange
        client = mock_titelive_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.encoding = "utf-8"
        client.http_client.request.return_value = mock_response

        # Act
        result = client.search_by_date(
            base="paper",
            min_date="01/01/2024",
            max_date="31/01/2024",
            page=2,
            results_per_page=50,
        )

        # Assert
        assert result == mock_api_response
        client.http_client.request.assert_called_once_with(
            "GET",
            "https://catsearch.epagine.fr/v1/search",
            headers={"Authorization": "Bearer test_token"},
            params={
                "base": "paper",
                "dateminm": "01/01/2024",
                "datemaxm": "31/01/2024",
                "nombre": "50",
                "page": "2",
            },
        )

    def test_search_by_date_without_max_date(
        self, mock_titelive_client, mock_api_response
    ):
        """Test search_by_date without max_date parameter."""
        # Arrange
        client = mock_titelive_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.encoding = "utf-8"
        client.http_client.request.return_value = mock_response

        # Act
        result = client.search_by_date(base="music", min_date="01/01/2024")

        # Assert
        assert result == mock_api_response
        called_params = client.http_client.request.call_args[1]["params"]
        assert "datemaxm" not in called_params
        assert called_params["base"] == "music"
        assert called_params["dateminm"] == "01/01/2024"

    def test_make_request_handles_token_expiration(self, mock_titelive_client):
        """Test that _make_request handles token expiration with retry."""
        # Arrange
        client = mock_titelive_client

        # First response: 401 (expired token)
        mock_response_401 = Mock()
        mock_response_401.status_code = 401

        # Second response: 200 (success after refresh)
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"result": "success"}
        mock_response_200.encoding = "utf-8"

        client.http_client.request.side_effect = [mock_response_401, mock_response_200]

        # Act
        result = client._make_request("GET", "https://test.com/api")

        # Assert
        assert result == {"result": "success"}
        assert client.http_client.request.call_count == 2
        client.token_manager.refresh_token.assert_called_once()

    def test_make_request_max_token_refresh_retries_exceeded(
        self, mock_titelive_client
    ):
        """Test that _make_request raises error after max token refresh retries."""
        # Arrange
        client = mock_titelive_client

        # All responses return 401
        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        client.http_client.request.return_value = mock_response_401

        # Act & Assert
        with pytest.raises(ValueError, match="Max token refresh retries .* exceeded"):
            client._make_request("GET", "https://test.com/api")

        # Should retry max_token_refresh_retries times (default is 2)
        assert client.http_client.request.call_count == 3  # Initial + 2 retries
        assert client.token_manager.refresh_token.call_count == 2

    def test_make_request_handles_none_response(self, mock_titelive_client):
        """Test that _make_request handles None response from http_client."""
        # Arrange
        client = mock_titelive_client
        client.http_client.request.return_value = None

        # Act & Assert
        with pytest.raises(
            requests.exceptions.RequestException, match="Request failed"
        ):
            client._make_request("GET", "https://test.com/api")

    def test_make_request_handles_none_response_after_token_refresh(
        self, mock_titelive_client
    ):
        """Test that _make_request handles None response after token refresh."""
        # Arrange
        client = mock_titelive_client

        # First response: 401, second response: None
        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        client.http_client.request.side_effect = [mock_response_401, None]

        # Act & Assert
        with pytest.raises(
            requests.exceptions.RequestException,
            match="Request failed after token refresh",
        ):
            client._make_request("GET", "https://test.com/api")

    def test_make_request_handles_token_refresh_failure(self, mock_titelive_client):
        """Test that _make_request propagates token refresh errors."""
        # Arrange
        client = mock_titelive_client

        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        client.http_client.request.return_value = mock_response_401
        client.token_manager.refresh_token.side_effect = Exception(
            "Token refresh failed"
        )

        # Act & Assert
        with pytest.raises(Exception, match="Token refresh failed"):
            client._make_request("GET", "https://test.com/api")

    def test_make_request_sets_response_encoding(self, mock_titelive_client):
        """Test that _make_request sets correct response encoding."""
        # Arrange
        client = mock_titelive_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}
        client.http_client.request.return_value = mock_response

        # Act
        client._make_request("GET", "https://test.com/api")

        # Assert
        assert mock_response.encoding == "utf-8"  # RESPONSE_ENCODING from config

    def test_make_request_with_params(self, mock_titelive_client):
        """Test that _make_request passes query parameters correctly."""
        # Arrange
        client = mock_titelive_client

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "success"}
        mock_response.encoding = "utf-8"
        client.http_client.request.return_value = mock_response

        params = {"key1": "value1", "key2": "value2"}

        # Act
        result = client._make_request("GET", "https://test.com/api", params=params)

        # Assert
        assert result == {"result": "success"}
        client.http_client.request.assert_called_once_with(
            "GET",
            "https://test.com/api",
            headers={"Authorization": "Bearer test_token"},
            params=params,
        )
