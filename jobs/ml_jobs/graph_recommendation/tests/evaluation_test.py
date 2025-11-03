"""Minimal tests for evaluation.py - NO database or file I/O"""

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config import EvaluationConfig
from src.evaluation import evaluate_embeddings


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


@contextmanager
def _mock_evaluation_pipeline():
    """Context manager that patches all evaluation pipeline functions.

    Yields a dictionary of all mock objects for test assertions.
    """
    with (
        patch("src.evaluation.load_and_index_embeddings") as mock_load_index,
        patch("src.evaluation.sample_test_items_lazy") as mock_sample,
        patch("src.evaluation.generate_predictions_lazy") as mock_generate,
        patch("pandas.read_parquet") as mock_load_parquet,
        patch("src.evaluation.join_retrieval_with_metadata") as mock_join,
        patch("src.evaluation.compute_all_scores_lazy") as mock_compute_scores,
        patch("src.evaluation.compute_evaluation_metrics") as mock_compute_metrics,
        patch("src.evaluation.logger"),
    ):
        yield {
            "load_and_index_embeddings": mock_load_index,
            "sample_test_items_lazy": mock_sample,
            "generate_predictions_lazy": mock_generate,
            "load_parquet": mock_load_parquet,
            "join_retrieval_with_metadata": mock_join,
            "compute_all_scores_lazy": mock_compute_scores,
            "compute_evaluation_metrics": mock_compute_metrics,
        }


def _setup_mock_returns(
    mocks,
    query_node_ids,
    retrieval_results,
    metadata,
    augmented_results,
    scored_results,
    metrics,
):
    """Configure return values for all mocked functions."""
    mock_table = MagicMock()
    mocks["load_and_index_embeddings"].return_value = mock_table
    mocks["sample_test_items_lazy"].return_value = query_node_ids
    mocks["generate_predictions_lazy"].return_value = retrieval_results
    mocks["load_parquet"].return_value = metadata
    mocks["join_retrieval_with_metadata"].return_value = augmented_results
    mocks["compute_all_scores_lazy"].return_value = scored_results
    mocks["compute_evaluation_metrics"].return_value = (metrics, scored_results)
    return mock_table


@pytest.fixture()
def mock_evaluation_pipeline(
    mock_query_node_ids,
    mock_retrieval_results,
    mock_metadata,
    mock_augmented_results,
    mock_scored_results,
    mock_metrics,
):
    """Fixture that provides fully configured evaluation pipeline mocks.

    Yields a tuple of (mocks dict, mock_table) for test assertions.
    """
    with _mock_evaluation_pipeline() as mocks:
        mock_table = _setup_mock_returns(
            mocks,
            mock_query_node_ids,
            mock_retrieval_results,
            mock_metadata,
            mock_augmented_results,
            mock_scored_results,
            mock_metrics,
        )
        yield mocks, mock_table


def test_evaluate_embeddings_no_db_creation(
    mock_evaluation_pipeline,
    mock_metrics,
    mock_scored_results,
):
    """Test evaluate_embeddings without creating any database or loading files."""
    mocks, _ = mock_evaluation_pipeline

    # Execute function
    metrics_df, results_df = evaluate_embeddings(
        raw_data_parquet_path="path/to/raw_data.parquet",
        embedding_parquet_path="path/to/embeddings.parquet",
        evaluation_config=EvaluationConfig(),
    )

    # Verify NO actual database operations happened
    # by checking mocks were called instead
    assert mocks[
        "load_and_index_embeddings"
    ].called, "Should have called mocked load_and_index_embeddings"
    assert mocks[
        "sample_test_items_lazy"
    ].called, "Should have called mocked sample_test_items_lazy"
    assert mocks[
        "generate_predictions_lazy"
    ].called, "Should have called mocked generate_predictions_lazy"
    assert mocks["load_parquet"].called, "Should have called mocked load_parquet"

    # Verify returns are correct
    assert isinstance(metrics_df, pd.DataFrame)
    assert isinstance(results_df, pd.DataFrame)
    pd.testing.assert_frame_equal(metrics_df, mock_metrics)
    pd.testing.assert_frame_equal(results_df, mock_scored_results)

    # Verify no rebuild was requested (default is False)
    _, kwargs = mocks["load_and_index_embeddings"].call_args
    assert kwargs["rebuild"] is False


def test_evaluate_embeddings_config_merging(
    mock_evaluation_pipeline,
    mock_metrics,
    mock_scored_results,
):
    """Test configuration merging without any I/O."""
    mocks, _ = mock_evaluation_pipeline

    # Custom config with overrides
    custom_config = {
        "n_samples": 50,
        "n_retrieved": 500,
        "rebuild_index": True,
        "force_artist_weight": True,
    }
    custom_config_jsons = json.dumps(custom_config)
    evaluation_config = EvaluationConfig().parse_and_update_config(custom_config_jsons)

    # Execute
    metrics_df, results_df = evaluate_embeddings(
        raw_data_parquet_path="fake.parquet",
        embedding_parquet_path="fake.parquet",
        evaluation_config=evaluation_config,
    )

    # Verify custom config was used
    mocks["sample_test_items_lazy"].assert_called_once_with(
        mocks["load_and_index_embeddings"].return_value, n_samples=50
    )

    # Check generate_predictions_lazy call
    call_args = mocks["generate_predictions_lazy"].call_args
    assert call_args[0][0] == ["node_1", "node_2", "node_3"]  # First positional arg
    assert call_args[1]["n_retrieved"] == 500  # Keyword arg

    # Check load_and_index_embeddings call
    load_call_kwargs = mocks["load_and_index_embeddings"].call_args[1]
    assert load_call_kwargs["rebuild"] is True

    # Check compute_all_scores_lazy call
    scores_call_kwargs = mocks["compute_all_scores_lazy"].call_args[1]
    assert scores_call_kwargs["force_artist_weight"] is True

    # Verify returns
    assert isinstance(metrics_df, pd.DataFrame)
    assert isinstance(results_df, pd.DataFrame)


def test_evaluate_embeddings_default_config_used(
    mock_evaluation_pipeline,
):
    """Test that default config values are used when no custom config provided."""
    mocks, _ = mock_evaluation_pipeline
    default_config = EvaluationConfig()

    # Execute with no custom config
    evaluate_embeddings(
        raw_data_parquet_path="fake.parquet",
        embedding_parquet_path="fake.parquet",
        evaluation_config=default_config,
    )

    # Verify default values were used
    mocks["sample_test_items_lazy"].assert_called_once_with(
        mocks["load_and_index_embeddings"].return_value,
        n_samples=default_config.n_samples,
    )

    generate_kwargs = mocks["generate_predictions_lazy"].call_args[1]
    assert generate_kwargs["n_retrieved"] == default_config.n_retrieved

    load_kwargs = mocks["load_and_index_embeddings"].call_args[1]
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
    default_config = EvaluationConfig()
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
    mock_evaluation_pipeline,
):
    """Test that metadata is loaded with correct filtering."""
    mocks, _ = mock_evaluation_pipeline
    default_config = EvaluationConfig()

    # Execute
    evaluate_embeddings(
        raw_data_parquet_path="fake.parquet",
        embedding_parquet_path="fake.parquet",
        evaluation_config=default_config,
    )

    # Verify load_metadata_table was called with correct parameters
    mocks["load_parquet"].assert_called_once()
    call_args = mocks["load_parquet"].call_args[0]
    call_kwargs = mocks["load_parquet"].call_args[1]

    assert call_args[0] == "fake.parquet"
    assert call_kwargs["columns"] == default_config.metadata_columns
