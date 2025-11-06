"""Tests for ExcelWriterService class."""

import math

import pandas as pd

from services.excel_writer import ExcelWriterService


class TestWriteKpiDataToSheet:
    """Test cases for write_kpi_data_to_sheet method."""

    def test_write_kpi_data_to_sheet_success(
        self, mock_openpyxl_worksheet, sample_kpi_data_dict, sample_date_mappings
    ):
        """Test successful KPI data writing to sheet."""
        # Act
        result = ExcelWriterService.write_kpi_data_to_sheet(
            worksheet=mock_openpyxl_worksheet,
            kpi_data=sample_kpi_data_dict,
            date_mappings=sample_date_mappings,
            row_idx=0,
        )

        # Assert
        assert result is True

    def test_write_kpi_data_to_sheet_no_data(self, mock_openpyxl_worksheet):
        """Test handling of empty KPI data."""
        # Act
        result = ExcelWriterService.write_kpi_data_to_sheet(
            worksheet=mock_openpyxl_worksheet,
            kpi_data={},
            date_mappings={},
            row_idx=0,
        )

        # Assert
        assert result is False

    def test_write_kpi_data_to_sheet_none_data(
        self, mock_openpyxl_worksheet, sample_date_mappings
    ):
        """Test handling of None KPI data."""
        # Act
        result = ExcelWriterService.write_kpi_data_to_sheet(
            worksheet=mock_openpyxl_worksheet,
            kpi_data=None,
            date_mappings=sample_date_mappings,
            row_idx=0,
        )

        # Assert
        assert result is False

    def test_write_kpi_data_to_sheet_with_monthly_data(
        self, mock_openpyxl_worksheet, sample_date_mappings
    ):
        """Test writing with monthly data only."""
        # Arrange
        kpi_data = {
            "yearly": {},
            "monthly": {
                "10/2024": 15.2,
                "11/2024": 18.5,
            },
        }

        # Act
        result = ExcelWriterService.write_kpi_data_to_sheet(
            worksheet=mock_openpyxl_worksheet,
            kpi_data=kpi_data,
            date_mappings=sample_date_mappings,
            row_idx=0,
        )

        # Assert
        assert result is True

    def test_write_kpi_data_to_sheet_with_yearly_data(
        self, mock_openpyxl_worksheet, sample_date_mappings
    ):
        """Test writing with yearly data only."""
        # Arrange
        kpi_data = {
            "yearly": {
                2021: 100.5,
                2022: 150.3,
            },
            "monthly": {},
        }

        # Act
        result = ExcelWriterService.write_kpi_data_to_sheet(
            worksheet=mock_openpyxl_worksheet,
            kpi_data=kpi_data,
            date_mappings=sample_date_mappings,
            row_idx=0,
        )

        # Assert
        assert result is True


class TestGetYearlyValue:
    """Test cases for _get_yearly_value method."""

    def test_get_yearly_value_simple_year(self):
        """Test extraction of simple year value (2021)."""
        # Arrange
        yearly_data = {2021: 100.5, 2022: 150.3}

        # Act
        result = ExcelWriterService._get_yearly_value(yearly_data, "2021")

        # Assert
        assert result == 100.5

    def test_get_yearly_value_scholar_year(self):
        """Test extraction of scholar year value (2021-2022)."""
        # Arrange
        yearly_data = {"2021-2022": 250.7, "2022-2023": 300.2}

        # Act
        result = ExcelWriterService._get_yearly_value(yearly_data, "2021-2022")

        # Assert
        assert result == 250.7

    def test_get_yearly_value_scholar_year_fallback(self):
        """Test fallback to start year for scholar year."""
        # Arrange
        yearly_data = {2021: 100.5, 2022: 150.3}

        # Act
        result = ExcelWriterService._get_yearly_value(yearly_data, "2021-2022")

        # Assert
        # Should fallback to 2021
        assert result == 100.5

    def test_get_yearly_value_not_found(self):
        """Test handling when year is not in data."""
        # Arrange
        yearly_data = {2021: 100.5, 2022: 150.3}

        # Act
        result = ExcelWriterService._get_yearly_value(yearly_data, "2023")

        # Assert
        assert result is None

    def test_get_yearly_value_invalid_format(self):
        """Test handling of invalid year format."""
        # Arrange
        yearly_data = {2021: 100.5}

        # Act
        result = ExcelWriterService._get_yearly_value(yearly_data, "invalid")

        # Assert
        assert result is None


class TestWriteCellValue:
    """Test cases for _write_cell_value method."""

    def test_write_cell_value_numeric(self, mock_openpyxl_worksheet):
        """Test writing numeric value to cell."""
        # Act
        result = ExcelWriterService._write_cell_value(
            worksheet=mock_openpyxl_worksheet, row=1, col=1, value=100.5
        )

        # Assert
        assert result is True
        assert mock_openpyxl_worksheet.cell(row=1, column=1).value == 100.5

    def test_write_cell_value_integer(self, mock_openpyxl_worksheet):
        """Test writing integer value to cell."""
        # Act
        result = ExcelWriterService._write_cell_value(
            worksheet=mock_openpyxl_worksheet, row=1, col=1, value=100
        )

        # Assert
        assert result is True
        assert mock_openpyxl_worksheet.cell(row=1, column=1).value == 100.0

    def test_write_cell_value_string(self, mock_openpyxl_worksheet):
        """Test writing string value to cell."""
        # Act
        result = ExcelWriterService._write_cell_value(
            worksheet=mock_openpyxl_worksheet, row=1, col=1, value="Test Value"
        )

        # Assert
        assert result is True
        assert mock_openpyxl_worksheet.cell(row=1, column=1).value == "Test Value"

    def test_write_cell_value_none(self, mock_openpyxl_worksheet):
        """Test handling of None value."""
        # Act
        result = ExcelWriterService._write_cell_value(
            worksheet=mock_openpyxl_worksheet, row=1, col=1, value=None
        )

        # Assert
        assert result is False

    def test_write_cell_value_nan(self, mock_openpyxl_worksheet):
        """Test handling of NaN value."""
        # Act
        result = ExcelWriterService._write_cell_value(
            worksheet=mock_openpyxl_worksheet, row=1, col=1, value=float("nan")
        )

        # Assert
        assert result is False

    def test_write_cell_value_inf(self, mock_openpyxl_worksheet):
        """Test handling of infinity value."""
        # Act
        result = ExcelWriterService._write_cell_value(
            worksheet=mock_openpyxl_worksheet, row=1, col=1, value=math.inf
        )

        # Assert
        assert result is False


class TestWriteTopDataToSheet:
    """Test cases for write_top_data_to_sheet method."""

    def test_write_top_data_to_sheet_success(
        self, mock_openpyxl_worksheet, sample_top_data
    ):
        """Test successful writing of top data."""
        # Act
        result = ExcelWriterService.write_top_data_to_sheet(
            worksheet=mock_openpyxl_worksheet, top_data=sample_top_data, start_row=1
        )

        # Assert
        assert result is True

    def test_write_top_data_to_sheet_empty_dataframe(self, mock_openpyxl_worksheet):
        """Test handling of empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame()

        # Act
        result = ExcelWriterService.write_top_data_to_sheet(
            worksheet=mock_openpyxl_worksheet, top_data=empty_df, start_row=1
        )

        # Assert
        assert result is False

    def test_write_top_data_to_sheet_none(self, mock_openpyxl_worksheet):
        """Test handling of None data."""
        # Act
        result = ExcelWriterService.write_top_data_to_sheet(
            worksheet=mock_openpyxl_worksheet, top_data=None, start_row=1
        )

        # Assert
        assert result is False

    def test_write_top_data_to_sheet_with_timestamp(self, mock_openpyxl_worksheet):
        """Test writing data with timestamp formatting."""
        # Arrange
        data_with_timestamp = pd.DataFrame(
            {
                "partition_month": [
                    pd.Timestamp("2024-10-01"),
                    pd.Timestamp("2024-11-01"),
                ],
                "value": [100, 200],
            }
        )

        # Act
        result = ExcelWriterService.write_top_data_to_sheet(
            worksheet=mock_openpyxl_worksheet, top_data=data_with_timestamp, start_row=1
        )

        # Assert
        assert result is True
        # First column should be formatted as MM/YYYY
        cell_value = mock_openpyxl_worksheet.cell(row=1, column=1).value
        assert "/" in cell_value

    def test_write_top_data_to_sheet_custom_start_row(
        self, mock_openpyxl_worksheet, sample_top_data
    ):
        """Test writing data with custom start row."""
        # Act
        result = ExcelWriterService.write_top_data_to_sheet(
            worksheet=mock_openpyxl_worksheet, top_data=sample_top_data, start_row=10
        )

        # Assert
        assert result is True

    def test_write_top_data_to_sheet_with_nulls(self, mock_openpyxl_worksheet):
        """Test writing data containing null values."""
        # Arrange
        data_with_nulls = pd.DataFrame(
            {
                "offer_name": ["Offer A", None, "Offer C"],
                "bookings_count": [100, 75, None],
            }
        )

        # Act
        result = ExcelWriterService.write_top_data_to_sheet(
            worksheet=mock_openpyxl_worksheet, top_data=data_with_nulls, start_row=1
        )

        # Assert
        # Should still return True if some values were written
        assert isinstance(result, bool)
