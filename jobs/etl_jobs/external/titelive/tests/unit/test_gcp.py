"""Tests for the GCP utility functions."""

from unittest.mock import Mock, patch

import pytest

from src.utils.gcp import access_secret_data


class TestGCPUtils:
    """Test cases for GCP utility functions."""

    @patch("src.utils.gcp.secretmanager.SecretManagerServiceClient")
    def test_access_secret_data_success(self, mock_client_class):
        """Test successful secret data access."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        mock_response.payload.data.decode.return_value = "test_secret_value"
        mock_client.access_secret_version.return_value = mock_response

        project_id = "test-project"
        secret_id = "test-secret"
        version_id = "latest"

        # Act
        result = access_secret_data(project_id, secret_id, version_id)

        # Assert
        assert result == "test_secret_value"
        expected_name = (
            f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        )
        mock_client.access_secret_version.assert_called_once_with(
            request={"name": expected_name}
        )
        mock_response.payload.data.decode.assert_called_once_with("UTF-8")

    @patch("src.utils.gcp.secretmanager.SecretManagerServiceClient")
    def test_access_secret_data_default_version(self, mock_client_class):
        """Test secret data access with default version."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_response = Mock()
        mock_response.payload.data.decode.return_value = "test_secret_value"
        mock_client.access_secret_version.return_value = mock_response

        project_id = "test-project"
        secret_id = "test-secret"

        # Act
        result = access_secret_data(project_id, secret_id)

        # Assert
        assert result == "test_secret_value"
        expected_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        mock_client.access_secret_version.assert_called_once_with(
            request={"name": expected_name}
        )

    @patch("src.utils.gcp.secretmanager.SecretManagerServiceClient")
    def test_access_secret_data_exception(self, mock_client_class):
        """Test exception handling in secret data access."""
        # Arrange
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.access_secret_version.side_effect = Exception("Secret not found")

        # Act & Assert
        with pytest.raises(Exception, match="Secret not found"):
            access_secret_data("test-project", "test-secret")
