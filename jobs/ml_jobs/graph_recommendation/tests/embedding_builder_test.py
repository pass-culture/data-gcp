"""Simple tests for embedding_builder module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import torch
from torch_geometric.data import HeteroData

from src.graph_recommendation.embedding_builder import (
    CONTEXT_SIZE,
    EMBEDDING_DIM,
    LEARNING_RATE,
    METAPATH,
    NUM_EPOCHS,
    NUM_NEGATIVE_SAMPLES,
    WALK_LENGTH,
    WALKS_PER_NODE,
    _train,
    train_metapath2vec,
)


def _build_sample_heterograph() -> HeteroData:
    """Build a minimal heterograph for testing."""
    data = HeteroData()

    # Add book nodes - num_nodes is inferred from x.shape[0]
    data["book"].x = torch.rand(3, 16)
    data["book"].num_nodes = 3  # Explicitly set num_nodes
    data.book_ids = ["book_1", "book_2", "book_3"]

    # Add metadata nodes
    data["artist_id"].x = torch.rand(2, 16)
    data["gtl_label_level_1"].x = torch.rand(2, 16)

    # Add edges
    data["book", "is_artist_id", "artist_id"].edge_index = torch.tensor(
        [[0, 1], [0, 1]]
    )
    data["artist_id", "artist_id_of", "book"].edge_index = torch.tensor(
        [[0, 1], [0, 1]]
    )
    data["book", "is_gtl_label_level_1", "gtl_label_level_1"].edge_index = torch.tensor(
        [[1, 2], [0, 1]]
    )
    data["gtl_label_level_1", "gtl_label_level_1_of", "book"].edge_index = torch.tensor(
        [[0, 1], [1, 2]]
    )

    # Add metadata tracking
    data.metadata_ids = [
        ("artist_id", "artist_1"),
        ("artist_id", "artist_2"),
        ("gtl_label_level_1", "Fiction"),
        ("gtl_label_level_1", "Comics"),
    ]
    data.metadata_columns = ["artist_id", "gtl_label_level_1"]

    return data


def test_embedding_constants_are_defined() -> None:
    """Test that all required constants are defined with expected values."""
    assert EMBEDDING_DIM == 128
    assert WALK_LENGTH == 56  # 14 * 4
    assert CONTEXT_SIZE == 10
    assert WALKS_PER_NODE == 5
    assert NUM_NEGATIVE_SAMPLES == 5
    assert NUM_EPOCHS == 15
    assert LEARNING_RATE == 0.01


def test_metapath_structure() -> None:
    """Test that METAPATH constant contains expected structure."""
    # Verify metapath is a list of tuples
    assert isinstance(METAPATH, list | tuple)
    assert all(isinstance(path, tuple) for path in METAPATH)
    assert all(len(path) == 3 for path in METAPATH)  # (source, edge, target)

    # Verify it contains book-related relations
    metapath_set = set(METAPATH)
    # Check that it includes artist and GTL relations
    assert any("artist_id" in str(path) for path in metapath_set)
    assert any("gtl_label_level" in str(path) for path in metapath_set)


def test_train_function_basic_execution() -> None:
    """Test that _train function executes without errors."""
    # Create minimal mock objects
    mock_model = MagicMock()
    mock_model.train = MagicMock()
    mock_model.loss.return_value = torch.tensor(0.5, requires_grad=True)

    # Simple data loader
    mock_loader = [
        (torch.tensor([[1, 2]]), torch.tensor([[3, 4]])),
        (torch.tensor([[5, 6]]), torch.tensor([[7, 8]])),
    ]

    mock_optimizer = MagicMock()
    device = "cpu"

    # Execute training function
    loss = _train(mock_model, mock_loader, mock_optimizer, device, profile=False)

    # Verify basic operations were called
    mock_model.train.assert_called_once()
    assert mock_optimizer.zero_grad.call_count == len(mock_loader)
    assert mock_optimizer.step.call_count == len(mock_loader)
    assert isinstance(loss, float)
    assert loss >= 0


def test_train_metapath2vec_with_minimal_mocking() -> None:
    """Test train_metapath2vec with minimal mocking - just prevent actual training."""
    graph_data = _build_sample_heterograph()

    # Mock only the heavy components to prevent actual ML training
    with (
        patch(
            "src.graph_recommendation.embedding_builder.MetaPath2Vec"
        ) as mock_metapath2vec,
        patch("src.graph_recommendation.embedding_builder.torch.save"),
        patch("src.graph_recommendation.embedding_builder.torch.load") as mock_load,
        patch("torch.optim.SparseAdam") as mock_optimizer_cls,
        patch("src.graph_recommendation.embedding_builder.ReduceLROnPlateau"),
        patch("src.graph_recommendation.embedding_builder._train") as mock_train,
        patch("src.graph_recommendation.embedding_builder.logger"),
    ):
        # Set up minimal model mock
        mock_model = MagicMock()
        mock_model.to.return_value.start = {"book": 0}
        mock_metapath2vec.return_value = mock_model

        # Mock optimizer to have param_groups
        mock_optimizer = MagicMock()
        mock_optimizer.param_groups = [{"lr": LEARNING_RATE}]
        mock_optimizer_cls.return_value = mock_optimizer

        # Mock the embedding tensor that would be loaded from checkpoint
        # Need to match the number of book nodes (3 in our test graph)
        num_book_nodes = len(graph_data.book_ids)
        # Create embeddings as a proper tensor that can be sliced
        mock_embeddings = torch.rand(num_book_nodes, EMBEDDING_DIM)

        # Mock torch.load to return checkpoint with proper structure
        def mock_load_fn(path, weights_only=False):
            return {"embedding.weight": mock_embeddings}

        mock_load.side_effect = mock_load_fn

        # Mock _train to return a loss value
        mock_train.return_value = 0.5

        # Run with minimal epochs
        with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
            result = train_metapath2vec(graph_data=graph_data, num_workers=0)

        # Verify we get a DataFrame result
        assert isinstance(result, pd.DataFrame)
        assert "node_ids" in result.columns
        assert "embeddings" in result.columns
        # Should have embeddings for all books
        assert len(result) == len(graph_data.book_ids)


def test_train_metapath2vec_parameter_acceptance() -> None:
    """Test that train_metapath2vec accepts different parameter combinations."""
    graph_data = _build_sample_heterograph()

    with (
        patch(
            "src.graph_recommendation.embedding_builder.MetaPath2Vec"
        ) as mock_metapath2vec,
        patch("src.graph_recommendation.embedding_builder.torch.save"),
        patch("src.graph_recommendation.embedding_builder.torch.load") as mock_load,
        patch("torch.optim.SparseAdam") as mock_optimizer_cls,
        patch("src.graph_recommendation.embedding_builder.ReduceLROnPlateau"),
        patch("src.graph_recommendation.embedding_builder._train") as mock_train,
        patch("src.graph_recommendation.embedding_builder.logger"),
    ):
        mock_model = MagicMock()
        mock_model.to.return_value.start = {"book": 0}
        mock_metapath2vec.return_value = mock_model

        # Mock optimizer to have param_groups
        mock_optimizer = MagicMock()
        mock_optimizer.param_groups = [{"lr": LEARNING_RATE}]
        mock_optimizer_cls.return_value = mock_optimizer

        # Need to match the number of book nodes (3 in our test graph)
        num_book_nodes = len(graph_data.book_ids)
        # Create embeddings as a proper tensor that can be sliced
        mock_embeddings = torch.rand(num_book_nodes, EMBEDDING_DIM)

        # Mock torch.load to return checkpoint with proper structure
        def mock_load_fn(path, weights_only=False):
            return {"embedding.weight": mock_embeddings}

        mock_load.side_effect = mock_load_fn

        # Mock _train to return a loss value
        mock_train.return_value = 0.5

        with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
            # Test with checkpoint path
            result = train_metapath2vec(
                graph_data=graph_data,
                checkpoint_path=Path("test.pt"),
                num_workers=2,
                profile=False,  # Avoid profiling division by zero in tests
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
