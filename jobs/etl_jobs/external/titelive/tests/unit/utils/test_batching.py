"""Tests for batching utilities."""

import pytest

from src.utils.batching import calculate_total_pages


class TestCalculateTotalPages:
    """Test cases for calculate_total_pages function."""

    def test_calculate_total_pages_exact_division(self):
        """Test calculation when total results divides evenly by per_page."""
        # Arrange
        total_results = 100
        per_page = 10

        # Act
        result = calculate_total_pages(total_results, per_page)

        # Assert
        assert result == 10

    def test_calculate_total_pages_with_remainder(self):
        """Test calculation when there's a remainder."""
        # Arrange
        total_results = 105
        per_page = 10

        # Act
        result = calculate_total_pages(total_results, per_page)

        # Assert
        assert result == 11

    def test_calculate_total_pages_invalid_per_page(self):
        """Test error handling when per_page is less than 1."""
        # Arrange
        total_results = 100
        per_page = 0

        # Act & Assert
        with pytest.raises(ValueError, match="Per page value must be at least 1"):
            calculate_total_pages(total_results, per_page)
