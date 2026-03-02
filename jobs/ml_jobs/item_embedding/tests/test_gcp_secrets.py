"""Unit tests for secrets module."""

from unittest.mock import MagicMock, patch

import pytest
from gcp_secrets import get_secret


class TestGetSecret:
    @patch("gcp_secrets.secretmanager.SecretManagerServiceClient")
    def test_get_secret_success(self, mock_client_cls):
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "my-token"
        mock_client_cls.return_value.access_secret_version.return_value = mock_response

        result = get_secret.__wrapped__("test_secret")  # bypass lru_cache

        assert result == "my-token"
        mock_client_cls.return_value.access_secret_version.assert_called_once()

    @patch("gcp_secrets.secretmanager.SecretManagerServiceClient")
    def test_get_secret_failure(self, mock_client_cls):
        mock_client_cls.return_value.access_secret_version.side_effect = Exception(
            "API error"
        )

        with pytest.raises(RuntimeError, match="Failed to retrieve secret"):
            get_secret.__wrapped__("bad_secret")
