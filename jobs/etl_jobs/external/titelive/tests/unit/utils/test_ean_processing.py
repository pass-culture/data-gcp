"""Tests for EAN processing utilities."""

import sys
from unittest.mock import Mock

import requests

# Mock http_tools before importing modules that depend on it
sys.modules["http_tools"] = Mock()
sys.modules["http_tools.clients"] = Mock()
sys.modules["http_tools.rate_limiters"] = Mock()

from src.utils.ean_processing import (  # noqa: E402
    _process_eans_by_base,
    process_eans_batch,
)


class TestProcessEansBatch:
    """Test cases for process_eans_batch function."""

    def test_process_eans_batch_with_music_and_paper(self, mock_titelive_client):
        """Test processing batch with both music and paper EANs."""
        # Arrange
        ean_pairs = [
            ("9781234567890", "SUPPORT_PHYSIQUE_MUSIQUE_CD"),  # Music
            ("9789876543210", "BOOK"),  # Paper
        ]

        # Mock get_by_eans_with_base method to return appropriate responses
        def side_effect_func(eans, base):
            if base == "music":
                return {
                    "result": [
                        {
                            "id": 123,
                            "article": {
                                "1": {"gencod": "9781234567890", "titre": "Music"}
                            },
                        }
                    ]
                }
            else:  # paper
                return {
                    "result": [
                        {
                            "id": 456,
                            "article": {
                                "1": {"gencod": "9789876543210", "titre": "Book"}
                            },
                        }
                    ]
                }

        mock_titelive_client.get_by_eans_with_base = Mock(side_effect=side_effect_func)

        # Act
        results = process_eans_batch(mock_titelive_client, ean_pairs, sub_batch_size=2)

        # Assert
        assert len(results) == 2
        assert all(r["status"] in ["processed", "deleted_in_titelive"] for r in results)
        # Both should be processed since mock returns both EANs
        processed = [r for r in results if r["status"] == "processed"]
        assert len(processed) == 2

    def test_process_eans_batch_music_only(
        self, mock_titelive_client, mock_api_response
    ):
        """Test processing batch with only music EANs."""
        # Arrange
        ean_pairs = [
            ("9781234567890", "SUPPORT_PHYSIQUE_MUSIQUE_CD"),
            ("9781234567891", "SUPPORT_PHYSIQUE_MUSIQUE_VINYLE"),
        ]

        # Mock get_by_eans_with_base method
        mock_titelive_client.get_by_eans_with_base = Mock(
            return_value=mock_api_response
        )

        # Act
        results = process_eans_batch(mock_titelive_client, ean_pairs, sub_batch_size=2)

        # Assert
        assert len(results) == 2

    def test_process_eans_batch_paper_only(
        self, mock_titelive_client, mock_api_response
    ):
        """Test processing batch with only paper EANs."""
        # Arrange
        ean_pairs = [
            ("9781234567890", "BOOK"),
            ("9789876543210", None),  # NULL subcategoryid defaults to paper
        ]

        # Mock get_by_eans_with_base method
        mock_titelive_client.get_by_eans_with_base = Mock(
            return_value=mock_api_response
        )

        # Act
        results = process_eans_batch(mock_titelive_client, ean_pairs, sub_batch_size=2)

        # Assert
        assert len(results) == 2


class TestProcessEansByBase:
    """Test cases for _process_eans_by_base function."""

    def test_process_eans_by_base_successful(
        self, mock_titelive_client, mock_api_response
    ):
        """Test successful processing of EANs."""
        # Arrange
        ean_pairs = [("9781234567890", "BOOK")]

        # Mock get_by_eans_with_base method
        mock_titelive_client.get_by_eans_with_base = Mock(
            return_value=mock_api_response
        )

        # Act
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=1
        )

        # Assert
        assert len(results) == 1
        assert results[0]["status"] == "processed"
        assert results[0]["ean"] == "9781234567890"
        assert results[0]["json_raw"] is not None

    def test_process_eans_by_base_with_deleted_eans(self, mock_titelive_client):
        """Test handling of EANs that are deleted (not returned by API)."""
        # Arrange
        ean_pairs = [("9781234567890", "BOOK"), ("9789876543210", "BOOK")]

        # Mock get_by_eans_with_base method with only one EAN returned
        mock_titelive_client.get_by_eans_with_base = Mock(
            return_value={
                "result": [
                    {
                        "id": 123,
                        "article": {"1": {"gencod": "9781234567890", "titre": "Test"}},
                    }
                ]
            }
        )

        # Act
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=2
        )

        # Assert
        assert len(results) == 2
        # One processed
        processed = [r for r in results if r["status"] == "processed"]
        assert len(processed) == 1
        assert processed[0]["ean"] == "9781234567890"
        # One deleted
        deleted = [r for r in results if r["status"] == "deleted_in_titelive"]
        assert len(deleted) == 1
        assert deleted[0]["ean"] == "9789876543210"
        assert deleted[0]["json_raw"] is None

    def test_process_eans_by_base_http_404_error_batch(self, mock_titelive_client):
        """Test handling of 404 error for a batch (processes individually)."""
        # Arrange
        ean_pairs = [("9781234567890", "BOOK"), ("9789876543210", "BOOK")]

        # Mock 404 response for batch, then individual responses
        mock_response_404 = Mock()
        mock_response_404.status_code = 404
        mock_response_success = Mock()
        mock_response_success.status_code = 200

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call (batch) returns 404
                error = requests.exceptions.HTTPError()
                error.response = mock_response_404
                raise error
            else:
                # Individual calls succeed
                return {
                    "result": [
                        {
                            "id": 123,
                            "article": {
                                "1": {
                                    "gencod": kwargs.get("params", {})
                                    .get("in", "")
                                    .replace("ean=", ""),
                                    "titre": "Test",
                                }
                            },
                        }
                    ]
                }

        mock_titelive_client._make_request = Mock(side_effect=side_effect)

        # Act
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=2
        )

        # Assert - Should have processed individually after 404
        assert len(results) == 2
        assert all(r["status"] == "processed" for r in results)

    def test_process_eans_by_base_http_404_error_single_ean(self, mock_titelive_client):
        """Test handling of 404 error for single EAN (marks as deleted)."""
        # Arrange
        ean_pairs = [("9781234567890", "BOOK")]

        # Mock 404 response
        mock_response_404 = Mock()
        mock_response_404.status_code = 404

        error = requests.exceptions.HTTPError()
        error.response = mock_response_404

        mock_titelive_client._make_request = Mock(side_effect=error)

        # Act
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=1
        )

        # Assert
        assert len(results) == 1
        assert results[0]["status"] == "deleted_in_titelive"
        assert results[0]["ean"] == "9781234567890"

    def test_process_eans_by_base_http_500_error(self, mock_titelive_client):
        """Test handling of HTTP 500 error (marks as failed)."""
        # Arrange
        ean_pairs = [("9781234567890", "BOOK")]

        # Mock 500 response
        mock_response_500 = Mock()
        mock_response_500.status_code = 500

        error = requests.exceptions.HTTPError()
        error.response = mock_response_500

        mock_titelive_client._make_request = Mock(side_effect=error)

        # Act
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=1
        )

        # Assert
        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert results[0]["ean"] == "9781234567890"

    def test_process_eans_by_base_generic_exception(self, mock_titelive_client):
        """Test handling of generic exception (marks as failed)."""
        # Arrange
        ean_pairs = [("9781234567890", "BOOK")]

        # Mock generic exception
        mock_titelive_client._make_request = Mock(
            side_effect=Exception("Network error")
        )

        # Act
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=1
        )

        # Assert
        assert len(results) == 1
        assert results[0]["status"] == "failed"
        assert results[0]["ean"] == "9781234567890"

    def test_process_eans_by_base_multiple_sub_batches(self, mock_titelive_client):
        """Test processing with multiple sub-batches."""
        # Arrange
        ean_pairs = [
            ("9781234567890", "BOOK"),
            ("9781234567891", "BOOK"),
            ("9781234567892", "BOOK"),
        ]

        # Mock get_by_eans_with_base method to return different responses per call
        # Batch 1: returns both EANs, Batch 2: returns one EAN
        responses = [
            {
                "result": [
                    {
                        "id": 123,
                        "article": {
                            "1": {"gencod": "9781234567890", "titre": "Book 1"}
                        },
                    },
                    {
                        "id": 124,
                        "article": {
                            "1": {"gencod": "9781234567891", "titre": "Book 2"}
                        },
                    },
                ]
            },
            {
                "result": [
                    {
                        "id": 125,
                        "article": {
                            "1": {"gencod": "9781234567892", "titre": "Book 3"}
                        },
                    }
                ]
            },
        ]
        mock_titelive_client.get_by_eans_with_base = Mock(side_effect=responses)

        # Act - sub_batch_size=2 means 2 batches (2 + 1)
        results = _process_eans_by_base(
            mock_titelive_client, ean_pairs, "paper", sub_batch_size=2
        )

        # Assert
        assert len(results) == 3
        # All should be processed
        assert all(r["status"] == "processed" for r in results)
