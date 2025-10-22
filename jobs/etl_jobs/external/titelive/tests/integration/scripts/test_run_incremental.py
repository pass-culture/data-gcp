"""Tests for run_incremental script."""

import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd
import pytest

# Mock http_tools before importing TiteliveClient
sys.modules["http_tools"] = Mock()
sys.modules["http_tools.clients"] = Mock()
sys.modules["http_tools.rate_limiters"] = Mock()

from config import MAX_SEARCH_RESULTS, TiteliveCategory  # noqa: E402
from src.scripts.run_incremental import (  # noqa: E402
    _format_date_for_api,
    run_incremental,
)


class TestFormatDateForApi:
    """Tests for _format_date_for_api function."""

    def test_converts_valid_date(self):
        """Test conversion of valid date from YYYY-MM-DD to DD/MM/YYYY."""
        result = _format_date_for_api("2024-10-15")
        assert result == "15/10/2024"

    def test_converts_date_with_leading_zeros(self):
        """Test conversion with leading zeros."""
        result = _format_date_for_api("2024-01-05")
        assert result == "05/01/2024"

    def test_converts_december_date(self):
        """Test conversion of December date."""
        result = _format_date_for_api("2024-12-31")
        assert result == "31/12/2024"

    def test_raises_error_for_invalid_format(self):
        """Test that invalid date format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _format_date_for_api("15/10/2024")

    def test_raises_error_for_invalid_date(self):
        """Test that invalid date raises ValueError."""
        with pytest.raises(ValueError, match="Invalid date format"):
            _format_date_for_api("2024-13-01")  # Invalid month

    def test_raises_error_for_empty_string(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError):
            _format_date_for_api("")


class TestRunIncremental:
    """Tests for run_incremental function."""

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_processes_single_date_single_base(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test processing a single date for a single base."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        # Mock last sync date (2 days ago for paper only)
        with (
            patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync,
            patch(
                "src.scripts.run_incremental.transform_api_response"
            ) as mock_transform,
            patch("src.scripts.run_incremental.insert_dataframe") as mock_insert,
            patch(
                "src.scripts.run_incremental.get_destination_table_schema"
            ) as mock_schema,
        ):
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

            def get_last_sync(client, table, provider, base):
                if base == TiteliveCategory.PAPER.value:
                    return two_days_ago
                return None  # Music has no sync history

            mock_last_sync.side_effect = get_last_sync

            # Mock API responses
            mock_api_client.search_by_date.side_effect = [
                {"nbreponses": 5},  # Initial metadata call
                {
                    "result": [{"id": 1, "article": {"1": {"gencod": "123"}}}] * 5
                },  # Page 1
            ]

            mock_transform.return_value = pd.DataFrame(
                {
                    "ean": ["123"],
                    "json_raw": ['{"test": "data"}'],
                }
            )

            mock_schema.return_value = []

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Should have called search_by_date (metadata + pages)
            assert mock_api_client.search_by_date.call_count >= 2
            # Should have inserted data
            mock_insert.assert_called()

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_processes_multiple_dates_both_bases(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test processing multiple dates for both paper and music bases."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        # Mock last sync dates (3 days ago for both)
        with (
            patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync,
            patch(
                "src.scripts.run_incremental.transform_api_response"
            ) as mock_transform,
            patch(
                "src.scripts.run_incremental.get_destination_table_schema"
            ) as mock_schema,
        ):
            three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
            mock_last_sync.return_value = three_days_ago

            # Mock API responses (empty results)
            mock_api_client.search_by_date.return_value = {"nbreponses": 0}
            mock_transform.return_value = pd.DataFrame()
            mock_schema.return_value = []

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Should have called search_by_date for both bases across 2 dates
            # (2 dates * 2 bases = 4 calls)
            assert mock_api_client.search_by_date.call_count >= 4

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_raises_error_when_exceeds_api_limit(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test that ValueError is raised when total results exceed API limit."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync:
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            mock_last_sync.return_value = two_days_ago

            # Mock API response with too many results
            mock_api_client.search_by_date.return_value = {
                "nbreponses": MAX_SEARCH_RESULTS + 100
            }

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            with pytest.raises(ValueError, match="exceeds API limit"):
                run_incremental(
                    target_table="project.dataset.target", project_id="test-project"
                )

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_handles_empty_results(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test handling when API returns no results."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with (
            patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync,
            patch("src.scripts.run_incremental.insert_dataframe") as mock_insert,
        ):
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            mock_last_sync.return_value = two_days_ago

            # Mock API response with no results
            mock_api_client.search_by_date.return_value = {"nbreponses": 0}

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Should not insert any data
            mock_insert.assert_not_called()

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_handles_pagination(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test that pagination works correctly."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with (
            patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync,
            patch(
                "src.scripts.run_incremental.transform_api_response"
            ) as mock_transform,
            patch(
                "src.scripts.run_incremental.get_destination_table_schema"
            ) as mock_schema,
            patch(
                "src.scripts.run_incremental.calculate_total_pages"
            ) as mock_calc_pages,
        ):
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

            def get_last_sync(client, table, provider, base):
                if base == TiteliveCategory.PAPER.value:
                    return two_days_ago
                return None

            mock_last_sync.side_effect = get_last_sync

            # Mock 250 results requiring 3 pages (120 per page)
            api_call_count = [0]

            def search_by_date_side_effect(*args, **kwargs):
                api_call_count[0] += 1
                if kwargs.get("results_per_page") == 0:
                    # Metadata call
                    return {"nbreponses": 250}
                else:
                    # Page call
                    return {"result": [{"id": 1, "article": {"1": {"gencod": "123"}}}]}

            mock_api_client.search_by_date.side_effect = search_by_date_side_effect
            mock_calc_pages.return_value = 3  # 3 pages

            mock_transform.return_value = pd.DataFrame(
                {
                    "ean": ["123"],
                    "json_raw": ['{"test": "data"}'],
                }
            )
            mock_schema.return_value = []

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            run_incremental(
                target_table="project.dataset.target",
                results_per_page=120,
                project_id="test-project",
            )

            # Should have called search_by_date: 1 metadata + 3 pages = 4 calls
            assert api_call_count[0] == 4

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_handles_no_dates_to_process(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test when all bases are already up to date."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync:
            # Mock last sync date as yesterday (so no dates to process)
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            mock_last_sync.return_value = yesterday

            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Should not call API
            mock_api_client.search_by_date.assert_not_called()

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_handles_api_errors_per_page(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test that errors on individual pages are logged but processing continues."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with (
            patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync,
            patch(
                "src.scripts.run_incremental.transform_api_response"
            ) as mock_transform,
            patch(
                "src.scripts.run_incremental.get_destination_table_schema"
            ) as mock_schema,
            patch(
                "src.scripts.run_incremental.calculate_total_pages"
            ) as mock_calc_pages,
        ):
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

            def get_last_sync(client, table, provider, base):
                if base == TiteliveCategory.PAPER.value:
                    return two_days_ago
                return None

            mock_last_sync.side_effect = get_last_sync

            api_call_count = [0]

            def search_by_date_side_effect(*args, **kwargs):
                api_call_count[0] += 1
                if kwargs.get("results_per_page") == 0:
                    # Metadata call
                    return {"nbreponses": 200}
                elif api_call_count[0] == 2:
                    # First page fails
                    raise Exception("API error")
                else:
                    # Other pages succeed
                    return {"result": [{"id": 1, "article": {"1": {"gencod": "123"}}}]}

            mock_api_client.search_by_date.side_effect = search_by_date_side_effect
            mock_calc_pages.return_value = 2

            mock_transform.return_value = pd.DataFrame(
                {
                    "ean": ["123"],
                    "json_raw": ['{"test": "data"}'],
                }
            )
            mock_schema.return_value = []

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            # Should not raise exception, just log and continue
            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Should have tried both pages despite error
            assert api_call_count[0] == 3  # 1 metadata + 2 pages

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_truncates_table_before_processing(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test that table is truncated before processing."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync:
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            mock_last_sync.return_value = two_days_ago

            # Mock API response with no results
            mock_api_client.search_by_date.return_value = {"nbreponses": 0}

            # Mock truncate query
            mock_query = Mock()
            mock_bq_client.query.return_value = mock_query
            mock_query.result.return_value = None

            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Should have called truncate
            assert any(
                "TRUNCATE TABLE" in str(call)
                for call in mock_bq_client.query.call_args_list
            )

    @patch("src.scripts.run_incremental.TiteliveClient")
    @patch("src.scripts.run_incremental.TokenManager")
    @patch("src.scripts.run_incremental.bigquery.Client")
    def test_adds_required_fields_to_dataframe(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test that required schema fields are added to transformed data."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client
        mock_api_client = Mock()
        mock_client_class.return_value = mock_api_client

        with (
            patch("src.scripts.run_incremental.get_last_sync_date") as mock_last_sync,
            patch(
                "src.scripts.run_incremental.transform_api_response"
            ) as mock_transform,
            patch("src.scripts.run_incremental.insert_dataframe") as mock_insert,
            patch(
                "src.scripts.run_incremental.get_destination_table_schema"
            ) as mock_schema,
        ):
            two_days_ago = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

            def get_last_sync(client, table, provider, base):
                if base == TiteliveCategory.PAPER.value:
                    return two_days_ago
                return None

            mock_last_sync.side_effect = get_last_sync

            # Mock API responses
            mock_api_client.search_by_date.side_effect = [
                {"nbreponses": 1},
                {"result": [{"id": 1, "article": {"1": {"gencod": "123"}}}]},
            ]

            mock_transform.return_value = pd.DataFrame(
                {
                    "ean": ["123"],
                    "json_raw": ['{"test": "data"}'],
                }
            )
            mock_schema.return_value = []

            # Mock truncate query
            mock_bq_client.query.return_value.result.return_value = None

            run_incremental(
                target_table="project.dataset.target", project_id="test-project"
            )

            # Check that insert was called with dataframe containing required fields
            mock_insert.assert_called()
            inserted_df = mock_insert.call_args[0][2]  # Third argument is the dataframe

            assert "status" in inserted_df.columns
            assert "processed_at" in inserted_df.columns
            assert "batch_number" in inserted_df.columns
            assert "subcategoryid" in inserted_df.columns
            assert "images_download_status" in inserted_df.columns
            assert "images_download_processed_at" in inserted_df.columns
