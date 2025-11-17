"""Tests for image download utilities."""

from unittest.mock import Mock, patch

import pytest
import requests

from src.utils.image_download import (
    _download_and_upload_to_gcs,
    _get_session,
    batch_download_and_upload,
)


class TestGetSession:
    """Test cases for _get_session function."""

    @patch("src.utils.image_download.HTTPAdapter")
    @patch("src.utils.image_download.Retry")
    @patch("src.utils.image_download.requests.Session")
    def test_get_session_creates_configured_session(
        self, mock_session_class, mock_retry_class, mock_adapter_class
    ):
        """Test _get_session creates session with retry and pooling."""
        # Arrange
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        mock_retry = Mock()
        mock_retry_class.return_value = mock_retry
        mock_adapter = Mock()
        mock_adapter_class.return_value = mock_adapter

        # Act
        result = _get_session(pool_connections=10, pool_maxsize=20, timeout=60)

        # Assert
        assert result == mock_session
        mock_session_class.assert_called_once()
        mock_session.mount.assert_any_call("https://", mock_adapter)


class TestDownloadAndUploadToGcs:
    """Test cases for _download_and_upload_to_gcs function."""

    def test_download_and_upload_to_gcs_success(
        self, mock_storage_client, mock_requests_session
    ):
        """Test successful download and upload to GCS."""
        # Arrange
        image_url = "https://example.com/image.jpg"
        gcs_path = "gs://test-bucket/path/to/image.jpg"

        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Act
        success, url, message = _download_and_upload_to_gcs(
            mock_storage_client, mock_requests_session, image_url, gcs_path
        )

        # Assert
        assert success is True
        assert url == image_url
        assert message == "Success"
        mock_requests_session.get.assert_called_once()
        mock_storage_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with("path/to/image.jpg")
        mock_blob.upload_from_string.assert_called_once()

    def test_download_and_upload_to_gcs_invalid_path_no_prefix(
        self, mock_storage_client, mock_requests_session
    ):
        """Test error handling for GCS path without gs:// prefix."""
        # Arrange
        image_url = "https://example.com/image.jpg"
        gcs_path = "bucket/path/to/image.jpg"

        # Act
        success, url, message = _download_and_upload_to_gcs(
            mock_storage_client, mock_requests_session, image_url, gcs_path
        )

        # Assert
        assert success is False
        assert url == image_url
        assert "Invalid GCS path format" in message

    def test_download_and_upload_to_gcs_invalid_path_structure(
        self, mock_storage_client, mock_requests_session
    ):
        """Test error handling for invalid GCS path structure."""
        # Arrange
        image_url = "https://example.com/image.jpg"
        gcs_path = "gs://bucket-only"

        # Act
        success, url, message = _download_and_upload_to_gcs(
            mock_storage_client, mock_requests_session, image_url, gcs_path
        )

        # Assert
        assert success is False
        assert url == image_url
        assert "Invalid GCS path structure" in message

    def test_download_and_upload_to_gcs_timeout_error(
        self, mock_storage_client, mock_requests_session
    ):
        """Test handling of timeout errors during download."""
        # Arrange
        image_url = "https://example.com/image.jpg"
        gcs_path = "gs://test-bucket/image.jpg"
        mock_requests_session.get.side_effect = requests.exceptions.Timeout(
            "Connection timeout"
        )

        # Act
        success, url, message = _download_and_upload_to_gcs(
            mock_storage_client, mock_requests_session, image_url, gcs_path
        )

        # Assert
        assert success is False
        assert url == image_url
        assert "Timeout error" in message

    def test_download_and_upload_to_gcs_connection_error(
        self, mock_storage_client, mock_requests_session
    ):
        """Test handling of connection errors during download."""
        # Arrange
        image_url = "https://example.com/image.jpg"
        gcs_path = "gs://test-bucket/image.jpg"
        mock_requests_session.get.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        # Act
        success, url, message = _download_and_upload_to_gcs(
            mock_storage_client, mock_requests_session, image_url, gcs_path
        )

        # Assert
        assert success is False
        assert url == image_url
        assert "Connection error" in message


class TestBatchDownloadAndUpload:
    """Test cases for batch_download_and_upload function."""

    @patch("src.utils.image_download.logger")
    @patch("src.utils.image_download.ThreadPoolExecutor")
    @patch("src.utils.image_download.tqdm")
    def test_batch_download_and_upload_success(
        self,
        mock_tqdm,
        mock_executor_class,
        mock_logger,
        mock_storage_client,
        mock_requests_session,
    ):
        """Test successful batch download and upload with ThreadPoolExecutor."""
        # Arrange
        image_urls = ["https://example.com/img1.jpg", "https://example.com/img2.jpg"]
        gcs_paths = ["gs://bucket/img1.jpg", "gs://bucket/img2.jpg"]
        max_workers = 50

        # Mock executor
        mock_executor = Mock()
        mock_executor_class.return_value.__enter__ = Mock(return_value=mock_executor)
        mock_executor_class.return_value.__exit__ = Mock(return_value=False)

        # Mock futures
        mock_future1 = Mock()
        mock_future1.result.return_value = (True, image_urls[0], "Success")
        mock_future2 = Mock()
        mock_future2.result.return_value = (True, image_urls[1], "Success")

        mock_executor.submit.side_effect = [mock_future1, mock_future2]
        mock_tqdm.return_value = [mock_future1, mock_future2]

        # Act
        with patch(
            "src.utils.image_download.as_completed",
            return_value=[mock_future1, mock_future2],
        ):
            result = batch_download_and_upload(
                mock_storage_client,
                mock_requests_session,
                image_urls,
                gcs_paths,
                max_workers,
            )

        # Assert
        assert len(result) == 2
        assert all(r[0] for r in result)  # All successful
        mock_executor_class.assert_called_once_with(max_workers=max_workers)

    @patch("src.utils.image_download.logger")
    def test_batch_download_and_upload_mismatched_lengths(
        self, mock_logger, mock_storage_client, mock_requests_session
    ):
        """Test error handling for mismatched URL and path lists."""
        # Arrange
        image_urls = ["https://example.com/img1.jpg"]
        gcs_paths = ["gs://bucket/img1.jpg", "gs://bucket/img2.jpg"]

        # Act & Assert
        with pytest.raises(
            ValueError, match="image_urls and gcs_paths must have the same length"
        ):
            batch_download_and_upload(
                mock_storage_client, mock_requests_session, image_urls, gcs_paths
            )
