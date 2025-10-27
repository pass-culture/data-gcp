"""Minimal tests for evaluation.py - NO database or file I/O"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.evaluation import DefaultEvaluationConfig, evaluate_embeddings


@pytest.fixture()
def mock_query_node_ids():
    """Sample query node IDs."""
    return ["node_1", "node_2", "node_3"]


@pytest.fixture()
def mock_retrieval_results():
    """Mock retrieval results DataFrame."""
    return pd.DataFrame(
        {
            "query_node_id": ["node_1", "node_1", "node_2"],
            "retrieved_node_id": ["node_10", "node_11", "node_20"],
            "distance": [0.1, 0.2, 0.15],
        }
    )


@pytest.fixture()
def mock_metadata():
    """Mock metadata DataFrame."""
    return pd.DataFrame(
        {
            "item_id": ["node_1", "node_2", "node_10", "node_11", "node_20"],
            "gtl_id": ["gtl_1", "gtl_2", "gtl_10", "gtl_11", "gtl_20"],
            "artist_id": [
                "artist_1",
                "artist_2",
                "artist_10",
                "artist_11",
                "artist_20",
            ],
        }
    )


@pytest.fixture()
def mock_augmented_results():
    """Mock results after joining with metadata."""
    return pd.DataFrame(
        {
            "query_node_id": ["node_1", "node_1", "node_2"],
            "retrieved_node_id": ["node_10", "node_11", "node_20"],
            "query_gtl_id": ["gtl_1", "gtl_1", "gtl_2"],
            "retrieved_gtl_id": ["gtl_10", "gtl_11", "gtl_20"],
            "query_artist_id": ["artist_1", "artist_1", "artist_2"],
            "retrieved_artist_id": ["artist_10", "artist_11", "artist_20"],
        }
    )


@pytest.fixture()
def mock_scored_results():
    """Mock results with computed scores."""
    return pd.DataFrame(
        {
            "query_node_id": ["node_1", "node_1", "node_2"],
            "retrieved_node_id": ["node_10", "node_11", "node_20"],
            "full_score": [0.8, 0.6, 0.9],
            "gtl_score": [0.7, 0.5, 0.85],
            "artist_score": [0.9, 0.7, 0.95],
        }
    )


@pytest.fixture()
def mock_metrics():
    """Mock evaluation metrics DataFrame."""
    return pd.DataFrame(
        {
            "metric": ["ndcg@10", "recall@10", "precision@10"],
            "value": [0.75, 0.80, 0.70],
        }
    )


def test_evaluate_embeddings_no_db_creation(
    mock_query_node_ids,
    mock_retrieval_results,
    mock_metadata,
    mock_augmented_results,
    mock_scored_results,
    mock_metrics,
):
    """Test evaluate_embeddings without creating any database or loading files."""

    # Mock ALL external functions to prevent any I/O
    with (
        patch("src.evaluation.load_and_index_embeddings") as mock_load_index,
        patch("src.evaluation.sample_test_items_lazy") as mock_sample,
        patch("src.evaluation.generate_predictions_lazy") as mock_generate,
        patch("src.evaluation.load_metadata_table") as mock_load_metadata,
        patch("src.evaluation.join_retrieval_with_metadata") as mock_join,
        patch("src.evaluation.compute_all_scores_lazy") as mock_compute_scores,
        patch("src.evaluation.compute_evaluation_metrics") as mock_compute_metrics,
        patch("src.evaluation.logger"),  # Mock logger to reduce noise
    ):
        # Setup all mocks to return our test data
        mock_table = MagicMock()
        mock_load_index.return_value = mock_table
        mock_sample.return_value = mock_query_node_ids
        mock_generate.return_value = mock_retrieval_results
        mock_load_metadata.return_value = mock_metadata
        mock_join.return_value = mock_augmented_results
        mock_compute_scores.return_value = mock_scored_results
        mock_compute_metrics.return_value = (mock_metrics, mock_scored_results)

        # Execute function
        metrics_df, results_df = evaluate_embeddings(
            raw_data_parquet_path="path/to/raw_data.parquet",
            embedding_parquet_path="path/to/embeddings.parquet",
        )

        # Verify NO actual database operations happened
        # by checking mocks were called instead
        assert (
            mock_load_index.called
        ), "Should have called mocked load_and_index_embeddings"
        assert mock_sample.called, "Should have called mocked sample_test_items_lazy"
        assert (
            mock_generate.called
        ), "Should have called mocked generate_predictions_lazy"
        assert (
            mock_load_metadata.called
        ), "Should have called mocked load_metadata_table"

        # Verify returns are correct
        assert isinstance(metrics_df, pd.DataFrame)
        assert isinstance(results_df, pd.DataFrame)
        pd.testing.assert_frame_equal(metrics_df, mock_metrics)
        pd.testing.assert_frame_equal(results_df, mock_scored_results)

        # Verify no rebuild was requested (default is False)
        _, kwargs = mock_load_index.call_args
        assert kwargs["rebuild"] is False


def test_evaluate_embeddings_config_merging(
    mock_query_node_ids,
    mock_retrieval_results,
    mock_metadata,
    mock_augmented_results,
    mock_scored_results,
    mock_metrics,
):
    """Test configuration merging without any I/O."""

    with (
        patch("src.evaluation.load_and_index_embeddings") as mock_load_index,
        patch("src.evaluation.sample_test_items_lazy") as mock_sample,
        patch("src.evaluation.generate_predictions_lazy") as mock_generate,
        patch("src.evaluation.load_metadata_table") as mock_load_metadata,
        patch("src.evaluation.join_retrieval_with_metadata") as mock_join,
        patch("src.evaluation.compute_all_scores_lazy") as mock_compute_scores,
        patch("src.evaluation.compute_evaluation_metrics") as mock_compute_metrics,
        patch("src.evaluation.logger"),
    ):
        # Setup minimal mocks
        mock_table = MagicMock()
        mock_load_index.return_value = mock_table
        mock_sample.return_value = mock_query_node_ids
        mock_generate.return_value = mock_retrieval_results
        mock_load_metadata.return_value = mock_metadata
        mock_join.return_value = mock_augmented_results
        mock_compute_scores.return_value = mock_scored_results
        mock_compute_metrics.return_value = (mock_metrics, mock_scored_results)

        # Custom config with overrides
        custom_config = {
            "n_samples": 50,
            "n_retrieved": 500,
            "rebuild_index": True,
            "force_artist_weight": True,
        }

        # Execute
        metrics_df, results_df = evaluate_embeddings(
            raw_data_parquet_path="fake.parquet",
            embedding_parquet_path="fake.parquet",
            eval_config=custom_config,
        )

        # Verify custom config was used
        mock_sample.assert_called_once_with(mock_table, n_samples=50)

        # Check generate_predictions_lazy call
        call_args = mock_generate.call_args
        assert call_args[0][0] == mock_query_node_ids  # First positional arg
        assert call_args[1]["n_retrieved"] == 500  # Keyword arg

        # Check load_and_index_embeddings call
        load_call_kwargs = mock_load_index.call_args[1]
        assert load_call_kwargs["rebuild"] is True

        # Check compute_all_scores_lazy call
        scores_call_kwargs = mock_compute_scores.call_args[1]
        assert scores_call_kwargs["force_artist_weight"] is True

        # Verify returns
        assert isinstance(metrics_df, pd.DataFrame)
        assert isinstance(results_df, pd.DataFrame)


def test_evaluate_embeddings_default_config_used(
    mock_query_node_ids,
    mock_retrieval_results,
    mock_metadata,
    mock_augmented_results,
    mock_scored_results,
    mock_metrics,
):
    """Test that default config values are used when no custom config provided."""

    with (
        patch("src.evaluation.load_and_index_embeddings") as mock_load_index,
        patch("src.evaluation.sample_test_items_lazy") as mock_sample,
        patch("src.evaluation.generate_predictions_lazy") as mock_generate,
        patch("src.evaluation.load_metadata_table") as mock_load_metadata,
        patch("src.evaluation.join_retrieval_with_metadata") as mock_join,
        patch("src.evaluation.compute_all_scores_lazy") as mock_compute_scores,
        patch("src.evaluation.compute_evaluation_metrics") as mock_compute_metrics,
        patch("src.evaluation.logger"),
    ):
        # Setup mocks
        mock_table = MagicMock()
        mock_load_index.return_value = mock_table
        mock_sample.return_value = mock_query_node_ids
        mock_generate.return_value = mock_retrieval_results
        mock_load_metadata.return_value = mock_metadata
        mock_join.return_value = mock_augmented_results
        mock_compute_scores.return_value = mock_scored_results
        mock_compute_metrics.return_value = (mock_metrics, mock_scored_results)
        default_config = DefaultEvaluationConfig()
        # Execute with no custom config
        evaluate_embeddings(
            raw_data_parquet_path="fake.parquet",
            embedding_parquet_path="fake.parquet",
        )

        # Verify default values were used
        mock_sample.assert_called_once_with(
            mock_table, n_samples=default_config.n_samples
        )

        generate_kwargs = mock_generate.call_args[1]
        assert generate_kwargs["n_retrieved"] == default_config.n_retrieved

        load_kwargs = mock_load_index.call_args[1]
        assert load_kwargs["rebuild"] == default_config.rebuild_index


def test_default_eval_config_structure():
    """Test DEFAULT_EVAL_CONFIG has correct structure."""
    required_keys = {
        "metadata_columns",
        "n_samples",
        "n_retrieved",
        "k_values",
        "relevance_thresholds",
        "ground_truth_score",
        "force_artist_weight",
        "rebuild_index",
    }
    default_config = DefaultEvaluationConfig()
    assert set(default_config.to_dict().keys()) == required_keys

    # Type checks
    assert isinstance(default_config.metadata_columns, list)
    assert isinstance(default_config.n_samples, int)
    assert isinstance(default_config.n_retrieved, int)
    assert isinstance(default_config.k_values, list)
    assert isinstance(default_config.relevance_thresholds, list)
    assert isinstance(default_config.ground_truth_score, str)
    assert isinstance(default_config.force_artist_weight, bool)
    assert isinstance(default_config.rebuild_index, bool)

    # Value checks
    assert default_config.n_samples > 0
    assert default_config.n_retrieved > 0
    assert len(default_config.k_values) > 0
    assert len(default_config.relevance_thresholds) > 0
    assert all(isinstance(k, int) for k in default_config.k_values)
    assert all(isinstance(t, float) for t in default_config.relevance_thresholds)


def test_evaluate_embeddings_metadata_filtering(
    mock_query_node_ids,
    mock_retrieval_results,
    mock_metadata,
    mock_augmented_results,
    mock_scored_results,
    mock_metrics,
):
    """Test that metadata is loaded with correct filtering."""

    with (
        patch("src.evaluation.load_and_index_embeddings") as mock_load_index,
        patch("src.evaluation.sample_test_items_lazy") as mock_sample,
        patch("src.evaluation.generate_predictions_lazy") as mock_generate,
        patch("src.evaluation.load_metadata_table") as mock_load_metadata,
        patch("src.evaluation.join_retrieval_with_metadata") as mock_join,
        patch("src.evaluation.compute_all_scores_lazy") as mock_compute_scores,
        patch("src.evaluation.compute_evaluation_metrics") as mock_compute_metrics,
        patch("src.evaluation.logger"),
    ):
        # Setup mocks
        mock_table = MagicMock()
        mock_load_index.return_value = mock_table
        mock_sample.return_value = mock_query_node_ids
        mock_generate.return_value = mock_retrieval_results
        mock_load_metadata.return_value = mock_metadata
        mock_join.return_value = mock_augmented_results
        mock_compute_scores.return_value = mock_scored_results
        mock_compute_metrics.return_value = (mock_metrics, mock_scored_results)

        # Execute
        evaluate_embeddings(
            raw_data_parquet_path="fake.parquet",
            embedding_parquet_path="fake.parquet",
        )

        # Verify load_metadata_table was called with correct parameters
        mock_load_metadata.assert_called_once()
        call_kwargs = mock_load_metadata.call_args[1]
        default_config = DefaultEvaluationConfig()

        assert call_kwargs["parquet_path"] == "fake.parquet"
        assert call_kwargs["filter_field"] == "item_id"
        assert "filter_values" in call_kwargs
        assert call_kwargs["columns"] == default_config.metadata_columns

        # Verify filter_values contains unique node IDs from retrieval results
        filter_values = call_kwargs["filter_values"]
        assert isinstance(filter_values, list)
        assert len(filter_values) > 0
