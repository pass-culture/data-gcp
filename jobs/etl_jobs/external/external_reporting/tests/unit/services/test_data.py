"""Tests for DataService class."""

from unittest.mock import patch

import pandas as pd
import pytest

from services.data import DataService


class TestDataService:
    """Test cases for DataService class."""

    def test_init(self, mock_duckdb_connection):
        """Test DataService initialization."""
        # Act
        service = DataService(mock_duckdb_connection)

        # Assert
        assert service.conn == mock_duckdb_connection

    def test_get_kpi_data_success(
        self, mock_duckdb_connection, sample_kpi_data, sample_kpi_data_with_year_label
    ):
        """Test successful KPI data retrieval."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        with (
            patch("services.data.query_yearly_kpi") as mock_yearly,
            patch("services.data.query_monthly_kpi") as mock_monthly,
        ):
            mock_yearly.return_value = sample_kpi_data_with_year_label
            mock_monthly.return_value = sample_kpi_data

            # Act
            result = service.get_kpi_data(
                kpi_name="test_kpi",
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-10-01",
                scope="individual",
                table_name="test_table",
            )

        # Assert
        assert result is not None
        assert "yearly" in result
        assert "monthly" in result
        assert isinstance(result["yearly"], dict)
        assert isinstance(result["monthly"], dict)

    def test_get_kpi_data_with_select_field(
        self, mock_duckdb_connection, sample_kpi_data, sample_kpi_data_with_year_label
    ):
        """Test KPI data retrieval with specific select field."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        with (
            patch("services.data.query_yearly_kpi") as mock_yearly,
            patch("services.data.query_monthly_kpi") as mock_monthly,
        ):
            mock_yearly.return_value = sample_kpi_data_with_year_label
            mock_monthly.return_value = sample_kpi_data

            # Act
            result = service.get_kpi_data(
                kpi_name="test_kpi",
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-10-01",
                scope="individual",
                table_name="test_table",
                select_field="numerator",
            )

        # Assert
        assert result is not None
        assert "yearly" in result
        assert "monthly" in result

    def test_get_kpi_data_with_agg_type(
        self, mock_duckdb_connection, sample_kpi_data, sample_kpi_data_with_year_label
    ):
        """Test KPI data retrieval with specific aggregation type."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        with (
            patch("services.data.query_yearly_kpi") as mock_yearly,
            patch("services.data.query_monthly_kpi") as mock_monthly,
        ):
            mock_yearly.return_value = sample_kpi_data_with_year_label
            mock_monthly.return_value = sample_kpi_data

            # Act
            result = service.get_kpi_data(
                kpi_name="test_kpi",
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-10-01",
                scope="individual",
                table_name="test_table",
                agg_type="max",
            )

        # Assert
        assert result is not None

    def test_get_kpi_data_error_handling(self, mock_duckdb_connection):
        """Test error handling in KPI data retrieval."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        with patch("services.data.query_yearly_kpi") as mock_yearly:
            mock_yearly.side_effect = Exception("Database error")

            # Act
            result = service.get_kpi_data(
                kpi_name="test_kpi",
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-10-01",
                scope="individual",
                table_name="test_table",
            )

        # Assert
        assert result is None


class TestGetTopRankings:
    """Test cases for get_top_rankings method."""

    def test_get_top_rankings_success(self, mock_duckdb_connection, sample_top_data):
        """Test successful top rankings retrieval."""
        # Arrange
        service = DataService(mock_duckdb_connection)
        mock_duckdb_connection.execute.return_value.df.return_value = sample_top_data

        # Act
        result = service.get_top_rankings(
            dimension_name="NAT",
            dimension_value="NAT",
            ds="2024-11-01",
            table_name="test_table",
            top_n=50,
            select_fields=["offer_name", "bookings_count"],
            ranking={"order_by": [{"field": "bookings_count", "direction": "DESC"}]},
        )

        # Assert
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_get_top_rankings_with_partition(
        self, mock_duckdb_connection, sample_top_data
    ):
        """Test top rankings with partition_by."""
        # Arrange
        service = DataService(mock_duckdb_connection)
        mock_duckdb_connection.execute.return_value.df.return_value = sample_top_data

        # Act
        result = service.get_top_rankings(
            dimension_name="REG",
            dimension_value="Île-de-France",
            ds="2024-11-01",
            table_name="test_table",
            top_n=10,
            select_fields=["offer_name", "bookings_count", "category"],
            ranking={
                "partition_by": ["category"],
                "order_by": [
                    {"field": "category", "direction": "ASC"},
                    {"field": "bookings_count", "direction": "DESC"},
                ],
            },
        )

        # Assert
        assert result is not None

    def test_get_top_rankings_empty_select_fields_error(self, mock_duckdb_connection):
        """Test error when select_fields is empty."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        # Act & Assert
        with pytest.raises(AssertionError):
            service.get_top_rankings(
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-11-01",
                table_name="test_table",
                top_n=50,
                select_fields=[],  # Empty
                ranking={
                    "order_by": [{"field": "bookings_count", "direction": "DESC"}]
                },
            )

    def test_get_top_rankings_missing_order_by_error(self, mock_duckdb_connection):
        """Test error when order_by is missing."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        # Act & Assert
        with pytest.raises(AssertionError):
            service.get_top_rankings(
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-11-01",
                table_name="test_table",
                top_n=50,
                select_fields=["offer_name"],
                ranking={},  # Missing order_by
            )

    def test_get_top_rankings_invalid_direction_error(self, mock_duckdb_connection):
        """Test error when direction is invalid."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        # Act & Assert
        with pytest.raises(AssertionError):
            service.get_top_rankings(
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-11-01",
                table_name="test_table",
                top_n=50,
                select_fields=["offer_name"],
                ranking={
                    "order_by": [{"field": "bookings_count", "direction": "INVALID"}]
                },
            )

    def test_get_top_rankings_partition_not_in_order_by_error(
        self, mock_duckdb_connection
    ):
        """Test error when partition_by field not in order_by."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        # Act & Assert
        with pytest.raises(AssertionError):
            service.get_top_rankings(
                dimension_name="NAT",
                dimension_value="NAT",
                ds="2024-11-01",
                table_name="test_table",
                top_n=50,
                select_fields=["offer_name", "category"],
                ranking={
                    "partition_by": ["category"],
                    "order_by": [
                        {"field": "bookings_count", "direction": "DESC"}
                    ],  # Missing category
                },
            )

    def test_get_top_rankings_database_error(self, mock_duckdb_connection):
        """Test error handling when database query fails."""
        # Arrange
        service = DataService(mock_duckdb_connection)
        mock_duckdb_connection.execute.side_effect = Exception("Database error")

        # Act
        result = service.get_top_rankings(
            dimension_name="NAT",
            dimension_value="NAT",
            ds="2024-11-01",
            table_name="test_table",
            top_n=50,
            select_fields=["offer_name"],
            ranking={"order_by": [{"field": "bookings_count", "direction": "DESC"}]},
        )

        # Assert
        assert result is None


class TestFormatMonthlyData:
    """Test cases for _format_monthly_data method."""

    def test_format_monthly_data_success(self, mock_duckdb_connection, sample_kpi_data):
        """Test successful monthly data formatting."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        # Act
        result = service._format_monthly_data(sample_kpi_data, "kpi")

        # Assert
        assert isinstance(result, dict)
        # Keys should be in MM/YYYY format
        for key in result.keys():
            assert "/" in key
            parts = key.split("/")
            assert len(parts) == 2
            assert len(parts[0]) == 2  # Month
            assert len(parts[1]) == 4  # Year

    def test_format_monthly_data_empty(self, mock_duckdb_connection):
        """Test formatting empty DataFrame."""
        # Arrange
        service = DataService(mock_duckdb_connection)
        empty_df = pd.DataFrame()

        # Act
        result = service._format_monthly_data(empty_df, "kpi")

        # Assert
        assert result == {}

    def test_format_monthly_data_invalid_select_field(
        self, mock_duckdb_connection, sample_kpi_data
    ):
        """Test that invalid select_field defaults to 'kpi'."""
        # Arrange
        service = DataService(mock_duckdb_connection)

        # Act
        result = service._format_monthly_data(sample_kpi_data, "invalid_field")

        # Assert - should use 'kpi' as fallback
        assert isinstance(result, dict)

    def test_format_monthly_data_with_nulls(self, mock_duckdb_connection):
        """Test formatting data with null values."""
        # Arrange
        service = DataService(mock_duckdb_connection)
        data_with_nulls = pd.DataFrame(
            {
                "partition_month": pd.to_datetime(["2024-01-01", "2024-02-01"]),
                "kpi": [100.0, None],
            }
        )

        # Act
        result = service._format_monthly_data(data_with_nulls, "kpi")

        # Assert
        # Should only include non-null values
        assert len(result) == 1
        assert "01/2024" in result
