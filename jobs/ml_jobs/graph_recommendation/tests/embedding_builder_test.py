from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import torch
from torch_geometric.data import HeteroData

from src.graph_recommendation.embedding_builder import (
    BATCH_SIZE,
    CONTEXT_SIZE,
    EMBEDDING_DIM,
    LEARNING_RATE,
    METAPATH,
    NUM_EPOCHS,
    NUM_NEGATIVE_SAMPLES,
    NUM_WORKERS,
    WALK_LENGTH,
    WALKS_PER_NODE,
    _train,
    train_metapath2vec,
)


def _build_sample_heterograph() -> HeteroData:
    """Build a minimal heterograph for testing."""
    graph_data = HeteroData()

    # Add book nodes
    graph_data["book"].num_nodes = 3
    graph_data.book_ids = ["book-1", "book-2", "book-3"]

    # Add metadata nodes
    graph_data["artist_id"].num_nodes = 2
    graph_data["gtl_label_level_1"].num_nodes = 2

    # Add edge indices (book -> metadata)
    graph_data["book", "is_artist_id", "artist_id"].edge_index = torch.tensor(
        [
            [0, 1, 2],  # book indices
            [0, 1, 0],  # artist indices
        ]
    )

    # Add reverse edge indices (metadata -> book)
    graph_data["artist_id", "artist_id_of", "book"].edge_index = torch.tensor(
        [
            [0, 1, 0],  # artist indices
            [0, 1, 2],  # book indices
        ]
    )

    # Add GTL level 1 edges
    graph_data[
        "book", "is_gtl_label_level_1", "gtl_label_level_1"
    ].edge_index = torch.tensor(
        [
            [0, 1, 2],  # book indices
            [0, 0, 1],  # gtl indices
        ]
    )

    graph_data[
        "gtl_label_level_1", "gtl_label_level_1_of", "book"
    ].edge_index = torch.tensor(
        [
            [0, 0, 1],  # gtl indices
            [0, 1, 2],  # book indices
        ]
    )

    return graph_data


def test_embedding_constants_are_defined() -> None:
    """Test that all required constants are properly defined."""
    assert isinstance(EMBEDDING_DIM, int)
    assert EMBEDDING_DIM > 0

    assert isinstance(WALK_LENGTH, int)
    assert WALK_LENGTH > 0

    assert isinstance(CONTEXT_SIZE, int)
    assert CONTEXT_SIZE > 0

    assert isinstance(WALKS_PER_NODE, int)
    assert WALKS_PER_NODE > 0

    assert isinstance(NUM_NEGATIVE_SAMPLES, int)
    assert NUM_NEGATIVE_SAMPLES > 0

    assert isinstance(NUM_EPOCHS, int)
    assert NUM_EPOCHS > 0

    assert isinstance(NUM_WORKERS, int)
    assert NUM_WORKERS >= 0

    assert isinstance(BATCH_SIZE, int)
    assert BATCH_SIZE > 0

    assert isinstance(LEARNING_RATE, float)
    assert LEARNING_RATE > 0


def test_metapath_structure() -> None:
    """Test that METAPATH has the correct structure."""
    assert isinstance(METAPATH, list | tuple)
    assert len(METAPATH) > 0

    # Each metapath step should be a tuple of (source_type, edge_type, dest_type)
    for step in METAPATH:
        assert isinstance(step, tuple)
        assert len(step) == 3
        src_type, edge_type, dst_type = step
        assert isinstance(src_type, str)
        assert isinstance(edge_type, str)
        assert isinstance(dst_type, str)

    # Check that metapath starts and ends with "book"
    assert METAPATH[0][0] == "book"  # First step starts from book
    assert METAPATH[-1][2] == "book"  # Last step ends at book


def test_metapath_contains_expected_relations() -> None:
    """Test that METAPATH contains expected relationship types."""
    metapath_str = str(METAPATH)

    # Should contain artist relations
    assert "is_artist_id" in metapath_str
    assert "artist_id_of" in metapath_str

    # Should contain GTL level relations
    assert "is_gtl_label_level_1" in metapath_str
    assert "gtl_label_level_1_of" in metapath_str
    assert "is_gtl_label_level_2" in metapath_str
    assert "gtl_label_level_2_of" in metapath_str


@patch("src.graph_recommendation.embedding_builder.torch.save")
@patch("src.graph_recommendation.embedding_builder.torch.load")
@patch("src.graph_recommendation.embedding_builder.MetaPath2Vec")
def test_train_metapath2vec_basic_functionality(
    mock_metapath2vec, mock_torch_load, mock_torch_save
) -> None:
    """Test basic functionality of train_metapath2vec without actual training."""
    # Setup mocks
    mock_model = MagicMock()
    mock_model.start = {"book": 0}
    mock_loader = MagicMock()
    mock_loader.__len__ = MagicMock(return_value=5)
    mock_model.loader.return_value = mock_loader
    mock_model.parameters.return_value = [torch.tensor([1.0])]
    mock_metapath2vec.return_value = mock_model

    # Mock the checkpoint loading
    mock_checkpoint = {"embedding.weight": torch.rand(5, EMBEDDING_DIM)}
    mock_torch_load.return_value = mock_checkpoint

    # Create test data
    graph_data = _build_sample_heterograph()

    # Run with minimal epochs to speed up test
    with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
        result = train_metapath2vec(graph_data=graph_data, num_workers=0)

    # Verify result structure
    assert isinstance(result, pd.DataFrame)
    assert "node_ids" in result.columns
    assert "embeddings" in result.columns
    assert len(result) == len(graph_data.book_ids)
    assert result["node_ids"].tolist() == graph_data.book_ids

    # Verify model was created with correct parameters
    mock_metapath2vec.assert_called_once()
    call_args = mock_metapath2vec.call_args
    assert call_args.kwargs["embedding_dim"] == EMBEDDING_DIM
    assert call_args.kwargs["metapath"] == METAPATH
    assert call_args.kwargs["walk_length"] == WALK_LENGTH
    assert call_args.kwargs["context_size"] == CONTEXT_SIZE
    assert call_args.kwargs["walks_per_node"] == WALKS_PER_NODE
    assert call_args.kwargs["num_negative_samples"] == NUM_NEGATIVE_SAMPLES


@patch("src.graph_recommendation.embedding_builder.torch.save")
@patch("src.graph_recommendation.embedding_builder.torch.load")
@patch("src.graph_recommendation.embedding_builder.MetaPath2Vec")
def test_train_metapath2vec_checkpoint_saving(
    mock_metapath2vec, mock_torch_load, mock_torch_save
) -> None:
    """Test that checkpoints are saved correctly."""
    # Setup mocks
    mock_model = MagicMock()
    mock_model.start = {"book": 0}
    mock_loader = MagicMock()
    mock_loader.__len__ = MagicMock(return_value=1)
    mock_loader.__iter__ = MagicMock(
        return_value=iter([(torch.tensor([[1, 2]]), torch.tensor([[3, 4]]))])
    )
    mock_model.loader.return_value = mock_loader
    mock_model.parameters.return_value = [torch.tensor([1.0])]
    mock_model.loss.return_value = torch.tensor(0.5)
    mock_metapath2vec.return_value = mock_model

    # Mock checkpoint loading
    mock_checkpoint = {"embedding.weight": torch.rand(5, EMBEDDING_DIM)}
    mock_torch_load.return_value = mock_checkpoint

    graph_data = _build_sample_heterograph()
    checkpoint_path = Path("test_checkpoint.pt")

    with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
        train_metapath2vec(
            graph_data=graph_data, checkpoint_path=checkpoint_path, num_workers=0
        )

    # Verify checkpoint was saved
    mock_torch_save.assert_called()
    save_calls = mock_torch_save.call_args_list
    assert any(call.args[1] == checkpoint_path for call in save_calls)


def test_train_metapath2vec_invalid_graph_data() -> None:
    """Test that train_metapath2vec handles invalid graph data appropriately."""
    # Create minimal graph without required edge types
    empty_graph = HeteroData()
    empty_graph["book"].num_nodes = 1
    empty_graph.book_ids = ["book-1"]

    # This should fail when trying to create MetaPath2Vec model
    with pytest.raises((KeyError, ValueError, RuntimeError)):
        train_metapath2vec(graph_data=empty_graph, num_workers=0)


def test_train_function_basic_structure() -> None:
    """Test the _train function structure without actual training."""
    # Create mock model, loader, optimizer
    mock_model = MagicMock()
    mock_model.train = MagicMock()
    mock_model.loss.return_value = torch.tensor(0.5)

    mock_loader = [
        (torch.tensor([[1, 2]]), torch.tensor([[3, 4]])),
        (torch.tensor([[5, 6]]), torch.tensor([[7, 8]])),
    ]

    mock_optimizer = MagicMock()
    device = "cpu"

    # Test without profiling
    loss = _train(mock_model, mock_loader, mock_optimizer, device, profile=False)

    # Verify basic training steps were called
    mock_model.train.assert_called_once()
    assert mock_optimizer.zero_grad.call_count == len(mock_loader)
    assert mock_optimizer.step.call_count == len(mock_loader)
    assert isinstance(loss, float)
    assert loss >= 0


def test_train_function_with_profiling() -> None:
    """Test the _train function with profiling enabled."""
    mock_model = MagicMock()
    mock_model.train = MagicMock()
    mock_model.loss.return_value = torch.tensor(0.3)

    mock_loader = [(torch.tensor([[1, 2]]), torch.tensor([[3, 4]]))]
    mock_optimizer = MagicMock()
    device = "cpu"

    # Test with profiling (should not raise errors)
    with patch("src.graph_recommendation.embedding_builder.logger") as mock_logger:
        loss = _train(mock_model, mock_loader, mock_optimizer, device, profile=True)

    # Verify profiling logged information
    assert mock_logger.info.call_count > 0
    assert isinstance(loss, float)


def test_device_selection() -> None:
    """Test that device selection works correctly."""
    graph_data = _build_sample_heterograph()

    with (
        patch("torch.cuda.is_available", return_value=True),
        patch(
            "src.graph_recommendation.embedding_builder.MetaPath2Vec"
        ) as mock_metapath2vec,
        patch("src.graph_recommendation.embedding_builder.torch.save"),
        patch(
            "src.graph_recommendation.embedding_builder.torch.load"
        ) as mock_torch_load,
    ):
        mock_model = MagicMock()
        mock_model.start = {"book": 0}
        mock_loader = MagicMock()
        mock_loader.__len__ = MagicMock(return_value=1)
        mock_model.loader.return_value = mock_loader
        mock_model.parameters.return_value = [torch.tensor([1.0])]
        mock_metapath2vec.return_value = mock_model

        mock_torch_load.return_value = {
            "embedding.weight": torch.rand(5, EMBEDDING_DIM)
        }

        with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
            train_metapath2vec(graph_data=graph_data, num_workers=0)

        # Verify model was moved to cuda device
        mock_model.to.assert_called_with("cuda")


def test_embedding_extraction_dimensions() -> None:
    """Test that extracted embeddings have correct dimensions."""
    graph_data = _build_sample_heterograph()

    with patch(  # noqa: SIM117
        "src.graph_recommendation.embedding_builder.MetaPath2Vec"
    ) as mock_metapath2vec:
        with patch("src.graph_recommendation.embedding_builder.torch.save"):
            with patch(
                "src.graph_recommendation.embedding_builder.torch.load"
            ) as mock_torch_load:
                mock_model = MagicMock()
                mock_model.start = {"book": 0}
                mock_loader = MagicMock()
                mock_loader.__len__ = MagicMock(return_value=1)
                mock_model.loader.return_value = mock_loader
                mock_model.parameters.return_value = [torch.tensor([1.0])]
                mock_metapath2vec.return_value = mock_model

                # Create mock embeddings with correct shape
                num_total_nodes = 10  # Total nodes across all types
                mock_embeddings = torch.rand(num_total_nodes, EMBEDDING_DIM)
                mock_torch_load.return_value = {"embedding.weight": mock_embeddings}

                with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
                    result = train_metapath2vec(graph_data=graph_data, num_workers=0)

                # Check embedding dimensions
                assert len(result) == len(graph_data.book_ids)
                for embedding in result["embeddings"]:
                    assert len(embedding) == EMBEDDING_DIM


def test_train_metapath2vec_parameters() -> None:
    """Test that train_metapath2vec accepts the expected parameters."""
    graph_data = _build_sample_heterograph()

    # Test with different parameters
    with patch(  # noqa: SIM117
        "src.graph_recommendation.embedding_builder.MetaPath2Vec"
    ) as mock_metapath2vec:
        with patch("src.graph_recommendation.embedding_builder.torch.save"):
            with patch(
                "src.graph_recommendation.embedding_builder.torch.load"
            ) as mock_torch_load:
                mock_model = MagicMock()
                mock_model.start = {"book": 0}
                mock_loader = MagicMock()
                mock_loader.__len__ = MagicMock(return_value=1)
                mock_model.loader.return_value = mock_loader
                mock_model.parameters.return_value = [torch.tensor([1.0])]
                mock_metapath2vec.return_value = mock_model

                mock_torch_load.return_value = {
                    "embedding.weight": torch.rand(5, EMBEDDING_DIM)
                }

                with patch("src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1):
                    # Test with custom checkpoint path
                    custom_path = Path("custom_checkpoint.pt")
                    result = train_metapath2vec(
                        graph_data=graph_data,
                        checkpoint_path=custom_path,
                        num_workers=2,
                        profile=True,
                    )

                assert isinstance(result, pd.DataFrame)


def test_checkpoint_directory_creation() -> None:
    """Test that checkpoint directory is created if it doesn't exist."""
    graph_data = _build_sample_heterograph()

    with patch(  # noqa: SIM117
        "src.graph_recommendation.embedding_builder.MetaPath2Vec"
    ) as mock_metapath2vec:
        with patch("src.graph_recommendation.embedding_builder.torch.save"):
            with patch(
                "src.graph_recommendation.embedding_builder.torch.load"
            ) as mock_torch_load:
                with patch("pathlib.Path.mkdir") as mock_mkdir:
                    mock_model = MagicMock()
                    mock_model.start = {"book": 0}
                    mock_loader = MagicMock()
                    mock_loader.__len__ = MagicMock(return_value=1)
                    mock_model.loader.return_value = mock_loader
                    mock_model.parameters.return_value = [torch.tensor([1.0])]
                    mock_metapath2vec.return_value = mock_model

                    mock_torch_load.return_value = {
                        "embedding.weight": torch.rand(5, EMBEDDING_DIM)
                    }

                    custom_path = Path("new_dir/checkpoint.pt")

                    with patch(
                        "src.graph_recommendation.embedding_builder.NUM_EPOCHS", 1
                    ):
                        train_metapath2vec(
                            graph_data=graph_data,
                            checkpoint_path=custom_path,
                            num_workers=0,
                        )

                    # Verify directory creation was called
                    mock_mkdir.assert_called_with(exist_ok=True)
