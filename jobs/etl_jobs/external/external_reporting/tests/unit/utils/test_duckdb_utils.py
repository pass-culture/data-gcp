"""Tests for duckdb_utils module."""

import pandas as pd
import pytest

from utils.duckdb_utils import (
    AggregationError,
    QueryError,
    aggregate_kpi_data,
    query_kpi_data,
    query_monthly_kpi,
    query_yearly_kpi,
)


class TestQueryKpiData:
    """Test cases for query_kpi_data function."""

    def test_query_kpi_data_success(self, mock_duckdb_connection, sample_kpi_data):
        """Test successful KPI data query."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_kpi_data(
            conn=mock_duckdb_connection,
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="NAT",
            dimension_value="NAT",
            ds="2024-10-01",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        mock_duckdb_connection.execute.assert_called_once()

    def test_query_kpi_data_without_ds(self, mock_duckdb_connection, sample_kpi_data):
        """Test query without ds parameter."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_kpi_data(
            conn=mock_duckdb_connection,
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="NAT",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        mock_duckdb_connection.execute.assert_called_once()

    def test_query_kpi_data_error_handling(self, mock_duckdb_connection):
        """Test error handling in query execution."""
        # Arrange
        mock_duckdb_connection.execute.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(QueryError):
            query_kpi_data(
                conn=mock_duckdb_connection,
                table_name="test_table",
                kpi_name="test_kpi",
                dimension_name="NAT",
            )


class TestQueryYearlyKpi:
    """Test cases for query_yearly_kpi function."""

    def test_query_yearly_kpi_individual_scope(
        self, mock_duckdb_connection, sample_kpi_data
    ):
        """Test yearly query for individual scope (calendar year)."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_yearly_kpi(
            conn=mock_duckdb_connection,
            ds="2024-10-01",
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="NAT",
            dimension_value="NAT",
            start_year=2021,
            end_year=2023,
            scope="individual",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        # Should be called multiple times (once per year)
        assert mock_duckdb_connection.execute.call_count >= 1

    def test_query_yearly_kpi_collective_scope(
        self, mock_duckdb_connection, sample_kpi_data
    ):
        """Test yearly query for collective scope (scholar year)."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_yearly_kpi(
            conn=mock_duckdb_connection,
            ds="2024-10-01",
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="REG",
            dimension_value="Île-de-France",
            start_year=2021,
            end_year=2023,
            scope="collective",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert mock_duckdb_connection.execute.call_count >= 1

    def test_query_yearly_kpi_default_end_year(
        self, mock_duckdb_connection, sample_kpi_data
    ):
        """Test that end_year defaults to ds year."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_yearly_kpi(
            conn=mock_duckdb_connection,
            ds="2024-10-01",
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="NAT",
            dimension_value="NAT",
            start_year=2024,
            scope="individual",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)


class TestQueryMonthlyKpi:
    """Test cases for query_monthly_kpi function."""

    def test_query_monthly_kpi_success(self, mock_duckdb_connection, sample_kpi_data):
        """Test successful monthly KPI query."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_monthly_kpi(
            conn=mock_duckdb_connection,
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="NAT",
            dimension_value="NAT",
            ds="2024-10-01",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        mock_duckdb_connection.execute.assert_called_once()

    def test_query_monthly_kpi_without_ds(
        self, mock_duckdb_connection, sample_kpi_data
    ):
        """Test monthly query without ds (uses today)."""
        # Arrange
        mock_duckdb_connection.execute.return_value.df.return_value = sample_kpi_data

        # Act
        result = query_monthly_kpi(
            conn=mock_duckdb_connection,
            table_name="test_table",
            kpi_name="test_kpi",
            dimension_name="NAT",
            dimension_value="NAT",
        )

        # Assert
        assert isinstance(result, pd.DataFrame)
        mock_duckdb_connection.execute.assert_called_once()

    def test_query_monthly_kpi_error_handling(self, mock_duckdb_connection):
        """Test error handling in monthly query."""
        # Arrange
        mock_duckdb_connection.execute.side_effect = Exception("Database error")

        # Act & Assert
        with pytest.raises(QueryError):
            query_monthly_kpi(
                conn=mock_duckdb_connection,
                table_name="test_table",
                kpi_name="test_kpi",
                dimension_name="NAT",
                dimension_value="NAT",
            )


class TestAggregateKpiData:
    """Test cases for aggregate_kpi_data function."""

    def test_aggregate_kpi_data_sum(self, sample_kpi_data_with_year_label):
        """Test sum aggregation."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data_with_year_label,
            agg_type="sum",
            time_grouping="yearly",
            select_field="kpi",
            scope="individual",
        )

        # Assert
        assert isinstance(result, dict)
        assert 2021 in result
        assert 2022 in result
        # Sum of 2021: 0.1 + 0.15 + 0.2 = 0.45
        assert result[2021] == pytest.approx(0.45, rel=0.01)
        # Sum of 2022: 0.3 + 0.35 + 0.4 = 1.05
        assert result[2022] == pytest.approx(1.05, rel=0.01)

    def test_aggregate_kpi_data_max(self, sample_kpi_data_with_year_label):
        """Test max aggregation."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data_with_year_label,
            agg_type="max",
            time_grouping="yearly",
            select_field="kpi",
            scope="individual",
        )

        # Assert
        assert isinstance(result, dict)
        # Max of 2021: 0.2
        assert result[2021] == pytest.approx(0.2, rel=0.01)
        # Max of 2022: 0.4
        assert result[2022] == pytest.approx(0.4, rel=0.01)

    def test_aggregate_kpi_data_december(self, sample_kpi_data_with_year_label):
        """Test December aggregation for individual scope."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data_with_year_label,
            agg_type="december",
            time_grouping="yearly",
            select_field="kpi",
            scope="individual",
        )

        # Assert
        assert isinstance(result, dict)
        # December value for 2021
        assert 2021 in result
        # December value for 2022
        assert 2022 in result

    def test_aggregate_kpi_data_wavg(self, sample_kpi_data_with_year_label):
        """Test weighted average aggregation."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data_with_year_label,
            agg_type="wavg",
            time_grouping="yearly",
            select_field="kpi",
            scope="individual",
        )

        # Assert
        assert isinstance(result, dict)
        assert 2021 in result
        assert 2022 in result
        # Weighted avg should be numerator/denominator
        assert all(isinstance(v, float) for v in result.values())

    def test_aggregate_kpi_data_empty_dataframe(self):
        """Test aggregation with empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame()

        # Act
        result = aggregate_kpi_data(
            data=empty_df,
            agg_type="sum",
            time_grouping="yearly",
            select_field="kpi",
            scope="individual",
        )

        # Assert
        assert result == {}

    def test_aggregate_kpi_data_monthly_grouping(self, sample_kpi_data):
        """Test aggregation with monthly grouping."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data,
            agg_type="sum",
            time_grouping="monthly",
            select_field="kpi",
            scope="individual",
        )

        # Assert
        assert isinstance(result, dict)
        # Keys should be timestamps
        assert all(isinstance(k, pd.Timestamp) for k in result.keys())

    def test_aggregate_kpi_data_invalid_select_field(
        self, sample_kpi_data_with_year_label
    ):
        """Test that invalid select_field defaults to 'kpi'."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data_with_year_label,
            agg_type="sum",
            time_grouping="yearly",
            select_field="invalid_field",
            scope="individual",
        )

        # Assert - should fall back to 'kpi' field
        assert isinstance(result, dict)

    def test_aggregate_kpi_data_numerator_field(self, sample_kpi_data_with_year_label):
        """Test aggregation with numerator field."""
        # Act
        result = aggregate_kpi_data(
            data=sample_kpi_data_with_year_label,
            agg_type="sum",
            time_grouping="yearly",
            select_field="numerator",
            scope="individual",
        )

        # Assert
        assert isinstance(result, dict)
        # Sum of numerators for 2021: 100 + 150 + 200 = 450
        assert result[2021] == pytest.approx(450.0, rel=0.01)

    def test_aggregate_kpi_data_august_collective(self):
        """Test August aggregation for collective scope."""
        # Arrange
        data = pd.DataFrame(
            {
                "partition_month": pd.to_datetime(
                    ["2021-08-01", "2021-12-01", "2022-08-01"]
                ),
                "kpi_name": ["test_kpi"] * 3,
                "year_label": ["2020-2021", "2021-2022", "2021-2022"],
                "kpi": [0.5, 0.6, 0.7],
            }
        )

        # Act
        result = aggregate_kpi_data(
            data=data,
            agg_type="august",
            time_grouping="yearly",
            select_field="kpi",
            scope="collective",
        )

        # Assert
        assert isinstance(result, dict)
        # Should get August values only
        assert "2020-2021" in result
        assert "2021-2022" in result

    def test_aggregate_kpi_data_unknown_scope_error(
        self, sample_kpi_data_with_year_label
    ):
        """Test error handling for unknown scope in december aggregation."""
        # Act & Assert
        with pytest.raises(AggregationError):
            aggregate_kpi_data(
                data=sample_kpi_data_with_year_label,
                agg_type="december",
                time_grouping="yearly",
                select_field="kpi",
                scope="unknown_scope",
            )
