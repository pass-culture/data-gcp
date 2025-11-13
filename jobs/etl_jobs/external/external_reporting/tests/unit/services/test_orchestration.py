"""Tests for ReportOrchestrationService class."""

from unittest.mock import Mock, patch

from services.orchestration import ReportOrchestrationService
from services.tracking import SheetStats


class TestReportOrchestrationService:
    """Test cases for ReportOrchestrationService class."""

    def test_init(self, mock_duckdb_connection):
        """Test service initialization."""
        # Act
        service = ReportOrchestrationService(mock_duckdb_connection)

        # Assert
        assert service.conn == mock_duckdb_connection
        assert service.data_service is not None

    def test_get_layout_type_top(self, mock_duckdb_connection):
        """Test layout type detection for top sheets."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)
        mock_sheet = Mock()
        mock_sheet.definition = "top_offers"

        # Act
        result = service._get_layout_type(mock_sheet)

        # Assert
        assert result == "top"

    def test_get_layout_type_kpis(self, mock_duckdb_connection):
        """Test layout type detection for KPI sheets."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)
        mock_sheet = Mock()
        mock_sheet.definition = "individual_kpis"

        # Act
        result = service._get_layout_type(mock_sheet)

        # Assert
        assert result == "kpis"

    def test_get_layout_type_other(self, mock_duckdb_connection):
        """Test layout type detection for other sheets."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)
        mock_sheet = Mock()
        mock_sheet.definition = "lexique"

        # Act
        result = service._get_layout_type(mock_sheet)

        # Assert
        assert result == "other"


class TestProcessAllSheets:
    """Test cases for process_all_sheets method."""

    def test_process_all_sheets_success(self, mock_duckdb_connection):
        """Test processing multiple sheets successfully."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        mock_sheet1 = Mock()
        mock_sheet1.definition = "individual_kpis"
        mock_sheet1.tab_name = "Part Individuelle"

        mock_sheet2 = Mock()
        mock_sheet2.definition = "top_offers"
        mock_sheet2.tab_name = "Top Offers"

        sheets = [mock_sheet1, mock_sheet2]

        # Act
        with patch.object(service, "_process_single_sheet") as mock_process:
            mock_process.return_value = SheetStats(
                sheet_name="Test", sheet_type="individual_kpis"
            )
            result = service.process_all_sheets(sheets, "2024-10-01")

        # Assert
        assert len(result) == 2
        assert all(isinstance(s, SheetStats) for s in result)

    def test_process_all_sheets_with_errors(self, mock_duckdb_connection):
        """Test processing sheets with errors."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        mock_sheet = Mock()
        mock_sheet.definition = "individual_kpis"
        mock_sheet.tab_name = "Part Individuelle"

        # Act
        with patch.object(service, "_process_single_sheet") as mock_process:
            mock_process.side_effect = Exception("Processing error")
            result = service.process_all_sheets([mock_sheet], "2024-10-01")

        # Assert
        assert len(result) == 1
        assert isinstance(result[0], SheetStats)


class TestParseKpiRow:
    """Test cases for _parse_kpi_row method."""

    def test_parse_kpi_row_valid(self, mock_duckdb_connection):
        """Test parsing valid KPI row."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        mock_cell1 = Mock()
        mock_cell1.value = "kpi_name=test_kpi"
        mock_cell2 = Mock()
        mock_cell2.value = "kpi"
        mock_cell3 = Mock()
        mock_cell3.value = "sum"

        row = [mock_cell1, mock_cell2, mock_cell3]

        # Act
        result = service._parse_kpi_row(row, 0)

        # Assert
        assert result is not None
        assert result["kpi_name"] == "test_kpi"
        assert result["select_field"] == "kpi"
        assert result["row_idx"] == 0

    def test_parse_kpi_row_invalid_format(self, mock_duckdb_connection):
        """Test parsing row with invalid format."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        mock_cell = Mock()
        mock_cell.value = "invalid_format"

        row = [mock_cell]

        # Act
        result = service._parse_kpi_row(row, 0)

        # Assert
        assert result is None

    def test_parse_kpi_row_empty(self, mock_duckdb_connection):
        """Test parsing empty row."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        mock_cell = Mock()
        mock_cell.value = None

        row = [mock_cell]

        # Act
        result = service._parse_kpi_row(row, 0)

        # Assert
        assert result is None


class TestMapAggregationType:
    """Test cases for _map_aggregation_type method."""

    def test_map_aggregation_type_valid(self, mock_duckdb_connection):
        """Test mapping valid aggregation type."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        # Act
        with patch(
            "services.orchestration.AGG_TYPE_MAPPING", {"Sum": "sum", "Max": "max"}
        ):
            result = service._map_aggregation_type("Sum")

        # Assert
        assert result == "sum"

    def test_map_aggregation_type_case_insensitive(self, mock_duckdb_connection):
        """Test case-insensitive mapping."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        # Act
        with patch("services.orchestration.AGG_TYPE_MAPPING", {"Sum": "sum"}):
            result = service._map_aggregation_type("SUM")

        # Assert
        assert result == "sum"

    def test_map_aggregation_type_invalid(self, mock_duckdb_connection):
        """Test mapping invalid aggregation type falls back to default."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        # Act
        with (
            patch("services.orchestration.AGG_TYPE_MAPPING", {"Sum": "sum"}),
            patch("services.orchestration.DEFAULT_AGG_TYPE", "sum"),
        ):
            result = service._map_aggregation_type("invalid")

        # Assert
        assert result == "sum"

    def test_map_aggregation_type_empty(self, mock_duckdb_connection):
        """Test mapping empty aggregation type."""
        # Arrange
        service = ReportOrchestrationService(mock_duckdb_connection)

        # Act
        with patch("services.orchestration.DEFAULT_AGG_TYPE", "sum"):
            result = service._map_aggregation_type("")

        # Assert
        assert result == "sum"
