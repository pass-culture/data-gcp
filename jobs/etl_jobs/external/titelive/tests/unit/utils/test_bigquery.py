"""Tests for BigQuery utilities."""

from unittest.mock import Mock, patch

import pandas as pd
from google.cloud import bigquery

from src.utils.bigquery import (
    count_failed_eans,
    count_failed_image_downloads,
    count_pending_image_downloads,
    create_destination_table,
    delete_failed_eans,
    fetch_batch_eans,
    fetch_batch_for_image_download,
    fetch_failed_eans,
    get_destination_table_schema,
    get_last_batch_number,
    insert_dataframe,
    update_image_download_results,
)


class TestGetDestinationTableSchema:
    """Test cases for get_destination_table_schema function."""

    def test_get_destination_table_schema_returns_required_fields(self):
        """Test that schema contains all required fields with correct types."""
        # Act
        schema = get_destination_table_schema()

        # Assert
        assert isinstance(schema, list)
        field_names = [field.name for field in schema]

        # Verify all required fields are present
        assert "ean" in field_names
        assert "subcategoryid" in field_names
        assert "status" in field_names
        assert "processed_at" in field_names
        assert "json_raw" in field_names
        assert "batch_number" in field_names
        assert "images_download_status" in field_names

        # Verify key field types
        schema_dict = {field.name: field for field in schema}
        assert schema_dict["ean"].field_type == "STRING"
        assert schema_dict["ean"].mode == "REQUIRED"
        assert schema_dict["batch_number"].field_type == "INTEGER"


class TestCreateDestinationTable:
    """Test cases for create_destination_table function."""

    @patch("src.utils.bigquery.logger")
    def test_create_destination_table_success(self, mock_logger, mock_bigquery_client):
        """Test successful table creation with clustering."""
        # Arrange
        table_id = "project.dataset.table"
        mock_table = Mock()
        mock_bigquery_client.create_table.return_value = mock_table
        mock_table.clustering_fields = ["ean"]

        # Act
        create_destination_table(mock_bigquery_client, table_id, drop_if_exists=False)

        # Assert
        mock_bigquery_client.create_table.assert_called_once()
        call_args = mock_bigquery_client.create_table.call_args
        created_table_arg = call_args.args[0]
        assert created_table_arg.clustering_fields == ["ean"]


class TestInsertDataframe:
    """Test cases for insert_dataframe function."""

    @patch("src.utils.bigquery.logger")
    def test_insert_dataframe_append_mode(self, mock_logger, mock_bigquery_client):
        """Test inserting dataframe in append mode."""
        # Arrange
        table_id = "project.dataset.table"
        df = pd.DataFrame({"ean": ["123", "456"], "status": ["processed", "processed"]})
        mock_job = Mock()
        mock_bigquery_client.load_table_from_dataframe.return_value = mock_job

        # Act
        insert_dataframe(mock_bigquery_client, table_id, df, mode="append")

        # Assert
        mock_bigquery_client.load_table_from_dataframe.assert_called_once()
        call_args = mock_bigquery_client.load_table_from_dataframe.call_args
        assert call_args.args[0].equals(df)
        assert call_args.args[1] == table_id
        job_config = call_args.kwargs.get("job_config")
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
        mock_job.result.assert_called_once()


class TestGetLastBatchNumber:
    """Test cases for get_last_batch_number function."""

    @patch("src.utils.bigquery.logger")
    def test_get_last_batch_number_with_data(self, mock_logger, mock_bigquery_client):
        """Test getting last batch number when table has data."""
        # Arrange
        destination_table = "project.dataset.table"
        mock_row = Mock()
        mock_row.last_batch = 5
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

        # Act
        result = get_last_batch_number(mock_bigquery_client, destination_table)

        # Assert
        assert result == 5

    @patch("src.utils.bigquery.logger")
    def test_get_last_batch_number_empty_table(self, mock_logger, mock_bigquery_client):
        """Test getting last batch number when table is empty."""
        # Arrange
        destination_table = "project.dataset.table"
        mock_row = Mock()
        mock_row.last_batch = -1
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

        # Act
        result = get_last_batch_number(mock_bigquery_client, destination_table)

        # Assert
        assert result == -1


class TestFetchBatchEans:
    """Test cases for fetch_batch_eans function."""

    @patch("src.utils.bigquery.logger")
    def test_fetch_batch_eans_success(self, mock_logger, mock_bigquery_client):
        """Test successful batch EAN fetching with OFFSET pagination."""
        # Arrange
        source_table = "project.dataset.source"
        batch_number = 2
        batch_size = 1000

        mock_row1 = Mock()
        mock_row1.ean = "123"
        mock_row1.subcategoryid = "1"
        mock_row2 = Mock()
        mock_row2.ean = "456"
        mock_row2.subcategoryid = "2"

        mock_bigquery_client.query.return_value.result.return_value = [
            mock_row1,
            mock_row2,
        ]

        # Act
        result = fetch_batch_eans(
            mock_bigquery_client, source_table, batch_number, batch_size
        )

        # Assert
        assert result == [("123", "1"), ("456", "2")]
        query_call = mock_bigquery_client.query.call_args[0][0]
        assert f"OFFSET {batch_number * batch_size}" in query_call
        assert f"LIMIT {batch_size}" in query_call


class TestCountFailedEans:
    """Test cases for count_failed_eans function."""

    @patch("src.utils.bigquery.logger")
    def test_count_failed_eans(self, mock_logger, mock_bigquery_client):
        """Test counting failed EANs."""
        # Arrange
        destination_table = "project.dataset.table"
        mock_row = Mock()
        mock_row.total = 42
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

        # Act
        result = count_failed_eans(mock_bigquery_client, destination_table)

        # Assert
        assert result == 42


class TestFetchFailedEans:
    """Test cases for fetch_failed_eans function."""

    @patch("src.utils.bigquery.logger")
    def test_fetch_failed_eans(self, mock_logger, mock_bigquery_client):
        """Test fetching failed EANs for reprocessing."""
        # Arrange
        destination_table = "project.dataset.table"
        batch_size = 100

        mock_row1 = Mock()
        mock_row1.ean = "111"
        mock_row1.subcategoryid = "1"
        mock_row2 = Mock()
        mock_row2.ean = "222"
        mock_row2.subcategoryid = "2"

        mock_bigquery_client.query.return_value.result.return_value = [
            mock_row1,
            mock_row2,
        ]

        # Act
        result = fetch_failed_eans(mock_bigquery_client, destination_table, batch_size)

        # Assert
        assert result == [("111", "1"), ("222", "2")]


class TestDeleteFailedEans:
    """Test cases for delete_failed_eans function."""

    @patch("src.utils.bigquery.logger")
    def test_delete_failed_eans(self, mock_logger, mock_bigquery_client):
        """Test deleting failed EANs."""
        # Arrange
        destination_table = "project.dataset.table"
        eans = ["123", "456", "789"]
        mock_job = Mock()
        mock_bigquery_client.query.return_value = mock_job

        # Act
        delete_failed_eans(mock_bigquery_client, destination_table, eans)

        # Assert
        query_call = mock_bigquery_client.query.call_args[0][0]
        assert "DELETE FROM" in query_call
        assert "WHERE ean IN" in query_call
        assert "AND status = 'failed'" in query_call
        mock_job.result.assert_called_once()

    @patch("src.utils.bigquery.logger")
    def test_delete_failed_eans_empty_list(self, mock_logger, mock_bigquery_client):
        """Test deleting failed EANs with empty list - should not query."""
        # Arrange
        destination_table = "project.dataset.table"
        eans = []

        # Act
        delete_failed_eans(mock_bigquery_client, destination_table, eans)

        # Assert
        mock_bigquery_client.query.assert_not_called()


class TestFetchBatchForImageDownload:
    """Test cases for fetch_batch_for_image_download function."""

    @patch("src.utils.bigquery.logger")
    def test_fetch_batch_for_image_download_pending(
        self, mock_logger, mock_bigquery_client
    ):
        """Test fetching pending image downloads."""
        # Arrange
        destination_table = "project.dataset.table"
        batch_number = 5

        mock_row = Mock()
        mock_row.ean = "123"
        mock_row.json_raw = '{"test": "data"}'
        mock_row.old_recto_image_uuid = "recto-uuid-123"
        mock_row.old_verso_image_uuid = "verso-uuid-123"

        mock_bigquery_client.query.return_value.result.return_value = [mock_row]

        # Act
        result = fetch_batch_for_image_download(
            mock_bigquery_client,
            destination_table,
            batch_number,
            reprocess_failed=False,
        )

        # Assert
        assert result == [
            {
                "ean": "123",
                "json_raw": '{"test": "data"}',
                "old_recto_image_uuid": "recto-uuid-123",
                "old_verso_image_uuid": "verso-uuid-123",
            }
        ]
        query_call = mock_bigquery_client.query.call_args[0][0]
        assert "images_download_status IS NULL" in query_call

    @patch("src.utils.bigquery.logger")
    def test_fetch_batch_for_image_download_failed(
        self, mock_logger, mock_bigquery_client
    ):
        """Test fetching failed image downloads for reprocessing."""
        # Arrange
        destination_table = "project.dataset.table"
        batch_number = 5

        mock_row = Mock()
        mock_row.ean = "456"
        mock_row.json_raw = '{"test": "data2"}'
        mock_row.old_recto_image_uuid = "recto-uuid-456"
        mock_row.old_verso_image_uuid = "verso-uuid-456"

        mock_bigquery_client.query.return_value.result.return_value = [mock_row]

        # Act
        result = fetch_batch_for_image_download(
            mock_bigquery_client, destination_table, batch_number, reprocess_failed=True
        )

        # Assert
        assert result == [
            {
                "ean": "456",
                "json_raw": '{"test": "data2"}',
                "old_recto_image_uuid": "recto-uuid-456",
                "old_verso_image_uuid": "verso-uuid-456",
            }
        ]
        query_call = mock_bigquery_client.query.call_args[0][0]
        assert "images_download_status = 'failed'" in query_call


class TestCountImageDownloads:
    """Test cases for counting image downloads."""

    @patch("src.utils.bigquery.logger")
    def test_count_pending_image_downloads(self, mock_logger, mock_bigquery_client):
        """Test counting pending image downloads."""
        # Arrange
        destination_table = "project.dataset.table"
        mock_row = Mock()
        mock_row.total = 150
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

        # Act
        result = count_pending_image_downloads(mock_bigquery_client, destination_table)

        # Assert
        assert result == 150

    @patch("src.utils.bigquery.logger")
    def test_count_failed_image_downloads(self, mock_logger, mock_bigquery_client):
        """Test counting failed image downloads."""
        # Arrange
        destination_table = "project.dataset.table"
        mock_row = Mock()
        mock_row.total = 25
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

        # Act
        result = count_failed_image_downloads(mock_bigquery_client, destination_table)

        # Assert
        assert result == 25


class TestUpdateImageDownloadResults:
    """Test cases for update_image_download_results function."""

    @patch("src.utils.bigquery.logger")
    def test_update_image_download_results_empty_list(
        self, mock_logger, mock_bigquery_client
    ):
        """Test update with empty results list - should not create temp table."""
        # Arrange
        destination_table = "project.dataset.table"
        results = []

        # Act
        update_image_download_results(mock_bigquery_client, destination_table, results)

        # Assert
        mock_bigquery_client.create_table.assert_not_called()
