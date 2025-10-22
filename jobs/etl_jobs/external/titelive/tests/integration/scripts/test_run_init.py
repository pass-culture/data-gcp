"""Tests for run_init script."""

import sys
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

# Mock http_tools before importing TiteliveClient
sys.modules["http_tools"] = Mock()
sys.modules["http_tools.clients"] = Mock()
sys.modules["http_tools.rate_limiters"] = Mock()

from config import MUSIC_SUBCATEGORIES, TiteliveCategory  # noqa: E402
from src.scripts.run_init import (  # noqa: E402
    _process_eans_by_base,
    process_eans_batch,
    run_init,
)


@pytest.fixture
def sample_ean_pairs_music():
    """Sample EAN pairs for music category."""
    music_subcat = next(iter(MUSIC_SUBCATEGORIES))  # Get first music subcategory
    return [
        ("9781111111111", music_subcat),
        ("9782222222222", music_subcat),
    ]


@pytest.fixture
def sample_ean_pairs_paper():
    """Sample EAN pairs for paper category."""
    return [
        ("9783333333333", "LIVRE"),
        ("9784444444444", "LIVRE"),
    ]


@pytest.fixture
def sample_ean_pairs_mixed():
    """Sample EAN pairs with mixed categories."""
    music_subcat = next(iter(MUSIC_SUBCATEGORIES))
    return [
        ("9781111111111", music_subcat),
        ("9782222222222", "LIVRE"),
        ("9783333333333", music_subcat),
        ("9784444444444", "LIVRE"),
    ]


@pytest.fixture
def mock_api_response_with_eans():
    """Mock API response containing specific EANs."""

    def _create_response(eans):
        result = []
        for i, ean in enumerate(eans):
            result.append(
                {
                    "id": i + 1,
                    "article": {
                        "1": {
                            "gencod": ean,
                            "datemodification": "15/10/2024",
                            "prix": 19.99,
                            "titre": f"Test Product {i}",
                        }
                    },
                }
            )
        return {"result": result}

    return _create_response


class TestProcessEansBatch:
    """Tests for process_eans_batch function."""

    def test_groups_eans_by_category(
        self, mock_titelive_client, sample_ean_pairs_mixed
    ):
        """Test that EANs are correctly grouped into music and paper categories."""
        with patch("src.scripts.run_init._process_eans_by_base") as mock_process:
            mock_process.return_value = []

            process_eans_batch(mock_titelive_client, sample_ean_pairs_mixed)

            # Should be called twice: once for music, once for paper
            assert mock_process.call_count == 2

            # Check that music EANs were processed
            music_call = next(
                call
                for call in mock_process.call_args_list
                if call[0][2] == TiteliveCategory.MUSIC
            )
            music_eans = music_call[0][1]
            assert len(music_eans) == 2

            # Check that paper EANs were processed
            paper_call = next(
                call
                for call in mock_process.call_args_list
                if call[0][2] == TiteliveCategory.PAPER
            )
            paper_eans = paper_call[0][1]
            assert len(paper_eans) == 2

    def test_processes_only_music_eans(
        self, mock_titelive_client, sample_ean_pairs_music
    ):
        """Test processing when only music EANs are provided."""
        with patch("src.scripts.run_init._process_eans_by_base") as mock_process:
            mock_process.return_value = []

            process_eans_batch(mock_titelive_client, sample_ean_pairs_music)

            # Should be called once for music only
            assert mock_process.call_count == 1
            assert mock_process.call_args[0][2] == TiteliveCategory.MUSIC

    def test_processes_only_paper_eans(
        self, mock_titelive_client, sample_ean_pairs_paper
    ):
        """Test processing when only paper EANs are provided."""
        with patch("src.scripts.run_init._process_eans_by_base") as mock_process:
            mock_process.return_value = []

            process_eans_batch(mock_titelive_client, sample_ean_pairs_paper)

            # Should be called once for paper only
            assert mock_process.call_count == 1
            assert mock_process.call_args[0][2] == TiteliveCategory.PAPER

    def test_handles_empty_ean_list(self, mock_titelive_client):
        """Test processing with empty EAN list."""
        with patch("src.scripts.run_init._process_eans_by_base") as mock_process:
            results = process_eans_batch(mock_titelive_client, [])

            # Should not process anything
            mock_process.assert_not_called()
            assert results == []


class TestProcessEansByBase:
    """Tests for _process_eans_by_base function."""

    def test_successful_processing(
        self, mock_titelive_client, sample_ean_pairs_paper, mock_api_response_with_eans
    ):
        """Test successful processing of EANs."""
        eans = [ean for ean, _ in sample_ean_pairs_paper]

        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.return_value = mock_api_response_with_eans(eans)

            with patch("src.scripts.run_init.transform_api_response") as mock_transform:
                mock_transform.return_value = pd.DataFrame(
                    {
                        "ean": eans,
                        "datemodification": ["15/10/2024", "15/10/2024"],
                        "json_raw": ['{"test": "data"}', '{"test": "data"}'],
                    }
                )

                results = _process_eans_by_base(
                    mock_titelive_client,
                    sample_ean_pairs_paper,
                    TiteliveCategory.PAPER,
                    250,
                )

                # Should have 2 processed results
                assert len(results) == 2
                assert all(r["status"] == "processed" for r in results)
                assert all(r["ean"] in eans for r in results)

    def test_marks_missing_eans_as_deleted(
        self, mock_titelive_client, sample_ean_pairs_paper, mock_api_response_with_eans
    ):
        """Test that missing EANs are marked as deleted_in_titelive."""
        # API returns only first EAN
        returned_eans = [sample_ean_pairs_paper[0][0]]

        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.return_value = mock_api_response_with_eans(returned_eans)

            with patch("src.scripts.run_init.transform_api_response") as mock_transform:
                mock_transform.return_value = pd.DataFrame(
                    {
                        "ean": returned_eans,
                        "datemodification": ["15/10/2024"],
                        "json_raw": ['{"test": "data"}'],
                    }
                )

                results = _process_eans_by_base(
                    mock_titelive_client,
                    sample_ean_pairs_paper,
                    TiteliveCategory.PAPER,
                    250,
                )

                # Should have 1 processed and 1 deleted
                assert len(results) == 2
                processed = [r for r in results if r["status"] == "processed"]
                deleted = [r for r in results if r["status"] == "deleted_in_titelive"]

                assert len(processed) == 1
                assert len(deleted) == 1
                assert deleted[0]["ean"] == sample_ean_pairs_paper[1][0]
                assert deleted[0]["datemodification"] is None
                assert deleted[0]["json_raw"] is None

    def test_handles_404_batch_error(
        self, mock_titelive_client, sample_ean_pairs_paper
    ):
        """Test that 404 errors on batch trigger individual processing."""
        # First call (batch) raises 404, subsequent calls (individual) succeed
        http_error = requests.exceptions.HTTPError()
        http_error.response = Mock()
        http_error.response.status_code = 404

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # First call (batch) raises 404
                raise http_error
            else:
                # Individual calls mark as deleted
                return {"result": []}

        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.side_effect = side_effect

            with patch("src.scripts.run_init.transform_api_response") as mock_transform:
                mock_transform.return_value = pd.DataFrame()

                results = _process_eans_by_base(
                    mock_titelive_client,
                    sample_ean_pairs_paper,
                    TiteliveCategory.PAPER,
                    250,
                )

                # Should have processed individually (3 calls: 1 batch + 2 individual)
                assert call_count[0] == 3
                # All should be marked as deleted
                assert len(results) == 2
                assert all(r["status"] == "deleted_in_titelive" for r in results)

    def test_handles_404_individual_error(self, mock_titelive_client):
        """Test that individual 404s are marked as deleted_in_titelive."""
        ean_pairs = [("9781111111111", "LIVRE")]

        http_error = requests.exceptions.HTTPError()
        http_error.response = Mock()
        http_error.response.status_code = 404

        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.side_effect = http_error

            results = _process_eans_by_base(
                mock_titelive_client,
                ean_pairs,
                TiteliveCategory.PAPER,
                1,  # Process individually
            )

            # Should be marked as deleted
            assert len(results) == 1
            assert results[0]["status"] == "deleted_in_titelive"
            assert results[0]["ean"] == "9781111111111"

    def test_handles_other_http_errors(
        self, mock_titelive_client, sample_ean_pairs_paper
    ):
        """Test handling of HTTP errors other than 404."""
        http_error = requests.exceptions.HTTPError()
        http_error.response = Mock()
        http_error.response.status_code = 500

        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.side_effect = http_error

            results = _process_eans_by_base(
                mock_titelive_client,
                sample_ean_pairs_paper,
                TiteliveCategory.PAPER,
                250,
            )

            # All EANs should be marked as failed
            assert len(results) == 2
            assert all(r["status"] == "failed" for r in results)
            assert all(r["datemodification"] is None for r in results)

    def test_handles_general_exceptions(
        self, mock_titelive_client, sample_ean_pairs_paper
    ):
        """Test handling of general exceptions."""
        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.side_effect = Exception("Network error")

            results = _process_eans_by_base(
                mock_titelive_client,
                sample_ean_pairs_paper,
                TiteliveCategory.PAPER,
                250,
            )

            # All EANs should be marked as failed
            assert len(results) == 2
            assert all(r["status"] == "failed" for r in results)

    def test_processes_in_sub_batches(
        self, mock_titelive_client, mock_api_response_with_eans
    ):
        """Test that large batches are split into sub-batches."""
        # Create 600 EANs (should be split into 3 sub-batches of 250)
        ean_pairs = [(f"978{str(i).zfill(10)}", "LIVRE") for i in range(600)]

        with patch.object(
            mock_titelive_client, "get_by_eans_with_base"
        ) as mock_get_eans:
            mock_get_eans.return_value = {"result": []}

            with patch("src.scripts.run_init.transform_api_response") as mock_transform:
                mock_transform.return_value = pd.DataFrame()

                _process_eans_by_base(
                    mock_titelive_client, ean_pairs, TiteliveCategory.PAPER, 250
                )

                # Should have called API 3 times (600 / 250 = 3 batches)
                assert mock_get_eans.call_count == 3


class TestRunInit:
    """Tests for run_init function."""

    @patch("src.scripts.run_init.TiteliveClient")
    @patch("src.scripts.run_init.TokenManager")
    @patch("src.scripts.run_init.bigquery.Client")
    def test_normal_mode(self, mock_bq_class, mock_token_class, mock_client_class):
        """Test run_init in normal mode."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client

        # Mock the helper functions
        with (
            patch("src.scripts.run_init.create_destination_table") as mock_create,
            patch("src.scripts.run_init.get_last_batch_number") as mock_get_batch,
            patch("src.scripts.run_init.fetch_batch_eans") as mock_fetch,
            patch("src.scripts.run_init.process_eans_batch") as mock_process,
            patch("src.scripts.run_init.insert_dataframe") as mock_insert,
            patch("src.scripts.run_init.get_destination_table_schema") as mock_schema,
        ):
            mock_get_batch.return_value = 0
            mock_fetch.side_effect = [
                [("9781111111111", "LIVRE")],  # First batch
                [],  # No more batches
            ]
            mock_process.return_value = [
                {
                    "ean": "9781111111111",
                    "subcategoryid": "LIVRE",
                    "status": "processed",
                    "datemodification": "15/10/2024",
                    "json_raw": '{"test": "data"}',
                }
            ]
            mock_schema.return_value = []

            run_init(
                source_table="project.dataset.source",
                destination_table="project.dataset.dest",
                project_id="test-project",
                resume=False,
                reprocess_failed=False,
            )

            # Should create table in normal mode
            mock_create.assert_called_once()
            # Should fetch batches
            assert mock_fetch.call_count == 2
            # Should process batch
            mock_process.assert_called_once()
            # Should insert results
            mock_insert.assert_called_once()

    @patch("src.scripts.run_init.TiteliveClient")
    @patch("src.scripts.run_init.TokenManager")
    @patch("src.scripts.run_init.bigquery.Client")
    def test_resume_mode(self, mock_bq_class, mock_token_class, mock_client_class):
        """Test run_init in resume mode."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client

        with (
            patch("src.scripts.run_init.create_destination_table") as mock_create,
            patch("src.scripts.run_init.get_last_batch_number") as mock_get_batch,
            patch("src.scripts.run_init.fetch_batch_eans") as mock_fetch,
        ):
            mock_get_batch.return_value = 5
            mock_fetch.return_value = []  # No more batches

            run_init(
                source_table="project.dataset.source",
                destination_table="project.dataset.dest",
                project_id="test-project",
                resume=True,
                reprocess_failed=False,
            )

            # Should NOT create table in resume mode
            mock_create.assert_not_called()
            # Should start from batch 6 (last + 1)
            mock_get_batch.assert_called_once()

    @patch("src.scripts.run_init.TiteliveClient")
    @patch("src.scripts.run_init.TokenManager")
    @patch("src.scripts.run_init.bigquery.Client")
    def test_reprocess_failed_mode(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test run_init in reprocess_failed mode."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client

        with (
            patch("src.scripts.run_init.create_destination_table") as mock_create,
            patch("src.scripts.run_init.get_last_batch_number") as mock_get_batch,
            patch("src.scripts.run_init.count_failed_eans") as mock_count,
            patch("src.scripts.run_init.fetch_failed_eans") as mock_fetch_failed,
            patch("src.scripts.run_init.delete_failed_eans") as mock_delete,
            patch("src.scripts.run_init.process_eans_batch") as mock_process,
            patch("src.scripts.run_init.insert_dataframe") as mock_insert,
            patch("src.scripts.run_init.get_destination_table_schema") as mock_schema,
        ):
            mock_get_batch.return_value = 0
            mock_count.return_value = 10
            mock_fetch_failed.side_effect = [
                [("9781111111111", "LIVRE")],  # First batch
                [],  # No more failed EANs
            ]
            mock_process.return_value = [
                {
                    "ean": "9781111111111",
                    "subcategoryid": "LIVRE",
                    "status": "processed",
                    "datemodification": "15/10/2024",
                    "json_raw": '{"test": "data"}',
                }
            ]
            mock_schema.return_value = []

            run_init(
                source_table="project.dataset.source",
                destination_table="project.dataset.dest",
                project_id="test-project",
                resume=False,
                reprocess_failed=True,
            )

            # Should NOT create table in reprocess mode
            mock_create.assert_not_called()
            # Should count failed EANs
            mock_count.assert_called_once()
            # Should fetch failed EANs
            assert mock_fetch_failed.call_count == 2
            # Should delete old failed records before inserting
            mock_delete.assert_called_once()
            # Should process and insert
            mock_process.assert_called_once()
            mock_insert.assert_called_once()

    @patch("src.scripts.run_init.TiteliveClient")
    @patch("src.scripts.run_init.TokenManager")
    @patch("src.scripts.run_init.bigquery.Client")
    def test_handles_no_failed_eans(
        self, mock_bq_class, mock_token_class, mock_client_class
    ):
        """Test reprocess mode when no failed EANs exist."""
        mock_bq_client = Mock()
        mock_bq_class.return_value = mock_bq_client

        with (
            patch("src.scripts.run_init.get_last_batch_number") as mock_get_batch,
            patch("src.scripts.run_init.count_failed_eans") as mock_count,
            patch("src.scripts.run_init.process_eans_batch") as mock_process,
        ):
            mock_get_batch.return_value = 0
            mock_count.return_value = 0  # No failed EANs

            run_init(
                source_table="project.dataset.source",
                destination_table="project.dataset.dest",
                project_id="test-project",
                reprocess_failed=True,
            )

            # Should not process anything
            mock_process.assert_not_called()
