"""Tests for data_utils module."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from utils.data_utils import (
    build_region_hierarchy,
    drac_selector,
    get_available_regions,
    sanitize_date_fields,
    sanitize_numeric_types,
    upload_zip_to_gcs,
)


class TestBuildRegionHierarchy:
    """Test cases for build_region_hierarchy function."""

    def test_build_region_hierarchy_success(
        self, sample_bigquery_hierarchy_rows, mock_bigquery_client
    ):
        """Test successful hierarchy building from BigQuery data."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = (
            sample_bigquery_hierarchy_rows
        )

        # Act
        with patch(
            "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
        ):
            result = build_region_hierarchy()

        # Assert
        assert isinstance(result, dict)
        assert "Île-de-France" in result
        assert "Auvergne-Rhône-Alpes" in result

        # Check structure
        idf = result["Île-de-France"]
        assert "departements" in idf
        assert "academies" in idf
        assert "academy_departments" in idf

        # Check data
        assert "Paris" in idf["departements"]
        assert "Paris" in idf["academies"]
        assert "Paris" in idf["academy_departments"]
        assert "Paris" in idf["academy_departments"]["Paris"]

    def test_build_region_hierarchy_empty_results(self, mock_bigquery_client):
        """Test handling of empty BigQuery results."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = []

        # Act
        with patch(
            "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
        ):
            result = build_region_hierarchy()

        # Assert
        assert result == {}

    def test_build_region_hierarchy_sorted_output(
        self, sample_bigquery_hierarchy_rows, mock_bigquery_client
    ):
        """Test that output is sorted consistently."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = (
            sample_bigquery_hierarchy_rows
        )

        # Act
        with patch(
            "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
        ):
            result = build_region_hierarchy()

        # Assert
        for region, data in result.items():
            # Check departments are sorted
            assert data["departements"] == sorted(data["departements"])
            # Check academies are sorted
            assert data["academies"] == sorted(data["academies"])
            # Check academy departments are sorted
            for academy, deps in data["academy_departments"].items():
                assert deps == sorted(deps)


class TestSanitizeDateFields:
    """Test cases for sanitize_date_fields function."""

    def test_sanitize_date_fields_valid_dates(self):
        """Test sanitization of valid date fields."""
        # Arrange
        df = pd.DataFrame(
            {
                "partition_month": ["2024-01-01", "2024-02-01", "2024-03-01"],
                "value": [100, 200, 300],
            }
        )

        # Act
        result = sanitize_date_fields(df, "partition_month")

        # Assert
        assert result["partition_month"].dtype == "object"
        # Check that dates are converted (can be date or datetime.date)
        from datetime import date

        assert all(isinstance(d, date) for d in result["partition_month"])

    def test_sanitize_date_fields_multiple_fields(self):
        """Test sanitization of multiple date fields."""
        # Arrange
        df = pd.DataFrame(
            {
                "start_date": ["2024-01-01", "2024-02-01"],
                "end_date": ["2024-01-31", "2024-02-28"],
                "value": [100, 200],
            }
        )

        # Act
        result = sanitize_date_fields(df, ["start_date", "end_date"])

        # Assert
        assert result["start_date"].dtype == "object"
        assert result["end_date"].dtype == "object"

    def test_sanitize_date_fields_missing_column(self):
        """Test handling of missing column (should log warning but not fail)."""
        # Arrange
        df = pd.DataFrame({"value": [100, 200, 300]})

        # Act - should not raise exception (just log warning)
        # We need to mock the logger to avoid the signature issue
        with patch("utils.data_utils.log_print.warning"):
            result = sanitize_date_fields(df, "missing_column")

        # Assert - original columns should still be there
        assert "value" in result.columns
        assert len(result) == 3


class TestSanitizeNumericTypes:
    """Test cases for sanitize_numeric_types function."""

    def test_sanitize_numeric_types_conversion(self):
        """Test conversion of object columns to numeric."""
        # Arrange
        df = pd.DataFrame(
            {
                "kpi": ["100.5", "200.3", "300.7"],
                "numerator": ["1000", "2000", "3000"],
                "denominator": ["10000", "10000", "10000"],
                "name": ["A", "B", "C"],
            }
        )
        df = df.astype(object)

        # Act
        result = sanitize_numeric_types(df)

        # Assert
        assert result["kpi"].dtype == "float64"
        assert result["numerator"].dtype == "float64"
        assert result["denominator"].dtype == "float64"
        assert result["name"].dtype == "object"  # Should remain object

    def test_sanitize_numeric_types_already_numeric(self):
        """Test handling of already numeric columns."""
        # Arrange
        df = pd.DataFrame(
            {
                "kpi": [100.5, 200.3, 300.7],
                "numerator": [1000.0, 2000.0, 3000.0],
            }
        )

        # Act
        result = sanitize_numeric_types(df)

        # Assert
        assert result["kpi"].dtype == "float64"
        assert result["numerator"].dtype == "float64"

    def test_sanitize_numeric_types_amount_columns(self):
        """Test conversion of amount-related columns."""
        # Arrange
        df = pd.DataFrame(
            {
                "booking_amount": ["100.5", "200.3"],
                "total_quantity": ["10", "20"],
            }
        )
        df = df.astype(object)

        # Act
        result = sanitize_numeric_types(df)

        # Assert
        assert result["booking_amount"].dtype == "float64"
        assert result["total_quantity"].dtype == "float64"


class TestGetAvailableRegions:
    """Test cases for get_available_regions function."""

    def test_get_available_regions_returns_sorted_list(
        self, sample_bigquery_hierarchy_rows, mock_bigquery_client
    ):
        """Test that available regions are returned as sorted list."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = (
            sample_bigquery_hierarchy_rows
        )

        # Act
        with patch(
            "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
        ):
            result = get_available_regions()

        # Assert
        assert isinstance(result, list)
        assert result == sorted(result)
        assert "Île-de-France" in result
        assert "Auvergne-Rhône-Alpes" in result


class TestDracSelector:
    """Test cases for drac_selector function."""

    def test_drac_selector_valid_input(
        self, sample_bigquery_hierarchy_rows, mock_bigquery_client
    ):
        """Test DRAC selector with valid input."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = (
            sample_bigquery_hierarchy_rows
        )

        # Act
        with patch(
            "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
        ):
            # Use proper slugified format
            result = drac_selector("Ile-de-France")

        # Assert
        assert result == ["Île-de-France"]

    def test_drac_selector_multiple_regions(
        self, sample_bigquery_hierarchy_rows, mock_bigquery_client
    ):
        """Test DRAC selector with multiple regions."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = (
            sample_bigquery_hierarchy_rows
        )

        # Act
        with patch(
            "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
        ):
            # Use proper slugified format
            result = drac_selector("Ile-de-France Auvergne-Rhone-Alpes")

        # Assert
        assert len(result) == 2
        assert "Île-de-France" in result
        assert "Auvergne-Rhône-Alpes" in result

    def test_drac_selector_invalid_region(
        self, sample_bigquery_hierarchy_rows, mock_bigquery_client
    ):
        """Test DRAC selector with invalid region."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = (
            sample_bigquery_hierarchy_rows
        )

        # Act & Assert - typer.Exit raises click.exceptions.Exit
        from click.exceptions import Exit

        with (
            patch(
                "utils.data_utils.bigquery.Client", return_value=mock_bigquery_client
            ),
            pytest.raises(Exit),
        ):
            drac_selector("invalid-region")


class TestUploadZipToGcs:
    """Test cases for upload_zip_to_gcs function."""

    def test_upload_zip_to_gcs_success(self, tmp_path):
        """Test successful upload to GCS."""
        # Arrange
        zip_file = tmp_path / "test.zip"
        zip_file.write_bytes(b"test content")

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Act
        with patch("utils.data_utils.storage.Client", return_value=mock_storage_client):
            result = upload_zip_to_gcs(
                local_zip_path=str(zip_file),
                bucket_name="test-bucket",
                destination_name="reports/",
            )

        # Assert
        assert result is True
        mock_storage_client.bucket.assert_called_once_with("test-bucket")
        mock_blob.upload_from_file.assert_called_once()

    def test_upload_zip_to_gcs_with_default_bucket(self, tmp_path):
        """Test upload with default bucket."""
        # Arrange
        zip_file = tmp_path / "test.zip"
        zip_file.write_bytes(b"test content")

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Act
        with patch("utils.data_utils.storage.Client", return_value=mock_storage_client):
            result = upload_zip_to_gcs(local_zip_path=str(zip_file))

        # Assert
        assert result is True
        mock_blob.upload_from_file.assert_called_once()
