"""Tests for CustomMetaPath2Vec model."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import torch

from src.custom_metapath2vec import CustomMetaPath2Vec

if TYPE_CHECKING:
    from torch_geometric.typing import EdgeType


@pytest.fixture()
def sample_graph_data() -> tuple[dict[EdgeType, torch.Tensor], list[list[EdgeType]]]:
    """Create a sample heterogeneous graph and metapaths for testing."""
    # Graph structure:
    # author (3 nodes: 0, 1, 2)
    # paper (2 nodes: 0, 1)
    # venue (1 node: 0)

    edge_index_dict = {
        ("author", "writes", "paper"): torch.tensor([[0, 1, 1, 2], [0, 0, 1, 1]]),
        ("paper", "written_by", "author"): torch.tensor([[0, 0, 1, 1], [0, 1, 1, 2]]),
        ("paper", "published_in", "venue"): torch.tensor([[0, 1], [0, 0]]),
        ("venue", "publishes", "paper"): torch.tensor([[0, 0], [0, 1]]),
    }

    metapaths = [
        [("author", "writes", "paper"), ("paper", "written_by", "author")],
        [
            ("author", "writes", "paper"),
            ("paper", "published_in", "venue"),
            ("venue", "publishes", "paper"),
            ("paper", "written_by", "author"),
        ],
    ]

    return edge_index_dict, metapaths


def test_initialization(sample_graph_data):
    """Test that the model initializes correctly with valid arguments."""
    edge_index_dict, metapaths = sample_graph_data
    model = CustomMetaPath2Vec(
        edge_index_dict=edge_index_dict,
        embedding_dim=16,
        metapaths=metapaths,
        walk_length=4,
        context_size=2,
        walks_per_node=2,
        num_negative_samples=2,
    )

    assert model.embedding_dim == 16
    assert len(model.metapaths) == 2
    assert model.walk_length == 4
    assert model.context_size == 2

    # Check num_nodes_dict calculation
    assert model.num_nodes_dict["author"] == 3
    assert model.num_nodes_dict["paper"] == 2
    assert model.num_nodes_dict["venue"] == 1

    # Check embedding size (total nodes + dummy node)
    # Total nodes = 3 + 2 + 1 = 6. Dummy node is at index 6.
    # Embedding weight size should be (7, 16)
    assert model.embedding.weight.shape == (7, 16)
    assert model.dummy_idx == 6


def test_validate_metapaths_success(sample_graph_data):
    """Test that valid metapaths pass validation."""
    _edge_index_dict, metapaths = sample_graph_data
    # Should not raise
    CustomMetaPath2Vec._validate_metapaths(metapaths)


def test_validate_metapaths_empty():
    """Test that empty metapaths list raises ValueError."""
    with pytest.raises(ValueError, match="At least one metapath must be provided"):
        CustomMetaPath2Vec._validate_metapaths([])


def test_validate_metapaths_different_start_type():
    """Test that metapaths with different start types raise ValueError."""
    metapaths = [
        [("author", "writes", "paper")],
        [("paper", "written_by", "author")],
    ]
    with pytest.raises(
        ValueError, match="All metapaths must start with the same node type"
    ):
        CustomMetaPath2Vec._validate_metapaths(metapaths)


def test_validate_metapaths_broken_chain():
    """Test that metapaths with disconnected edges raise ValueError."""
    metapaths = [[("author", "writes", "paper"), ("venue", "publishes", "paper")]]
    with pytest.raises(ValueError, match="Found invalid metapath"):
        CustomMetaPath2Vec._validate_metapaths(metapaths)


def test_forward(sample_graph_data):
    """Test the forward pass returns embeddings of correct shape."""
    edge_index_dict, metapaths = sample_graph_data
    model = CustomMetaPath2Vec(
        edge_index_dict=edge_index_dict,
        embedding_dim=16,
        metapaths=metapaths,
        walk_length=4,
        context_size=2,
    )

    # Test forward for 'author' nodes
    batch = torch.tensor([0, 1, 2])
    emb = model("author", batch)
    assert emb.shape == (3, 16)

    # Test forward for 'paper' nodes
    batch = torch.tensor([0, 1])
    emb = model("paper", batch)
    assert emb.shape == (2, 16)


def test_sample_and_loss(sample_graph_data):
    """Test sampling and loss computation."""
    edge_index_dict, metapaths = sample_graph_data
    model = CustomMetaPath2Vec(
        edge_index_dict=edge_index_dict,
        embedding_dim=16,
        metapaths=metapaths,
        walk_length=4,
        context_size=2,
        walks_per_node=1,
        num_negative_samples=1,
    )

    loader = model.loader(batch_size=2, shuffle=False)
    batch = next(iter(loader))
    pos_rw, neg_rw = batch

    assert isinstance(pos_rw, torch.Tensor)
    assert isinstance(neg_rw, torch.Tensor)

    # Check that we got some samples
    # Note: exact shape depends on how many walks were valid (didn't hit dummy nodes)
    # But we should have some
    assert pos_rw.size(0) > 0
    assert neg_rw.size(0) > 0

    loss = model.loss(pos_rw, neg_rw)
    assert isinstance(loss, torch.Tensor)
    assert not torch.isnan(loss)
    assert loss.item() >= 0


def test_get_node_type_ranges(sample_graph_data):
    """Test node type range calculation."""
    _edge_index_dict, metapaths = sample_graph_data

    # Manually construct num_nodes_dict
    num_nodes_dict = {"author": 3, "paper": 2, "venue": 1}

    start, end, count = CustomMetaPath2Vec._get_node_type_ranges(
        metapaths, num_nodes_dict
    )

    # Types are sorted alphabetically: author, paper, venue
    # author: 0-3
    # paper: 3-5
    # venue: 5-6

    assert start["author"] == 0
    assert end["author"] == 3

    assert start["paper"] == 3
    assert end["paper"] == 5

    assert start["venue"] == 5
    assert end["venue"] == 6

    assert count == 6


def test_pos_sample_prunes_dummy_nodes():
    """Test that walks containing dummy nodes are pruned."""
    # Graph structure:
    # author (2 nodes: 0, 1)
    # paper (1 node: 0)
    # Author 0 writes Paper 0.
    # Author 1 writes nothing (dead end).

    edge_index_dict = {
        ("author", "writes", "paper"): torch.tensor([[0], [0]]),
        ("paper", "written_by", "author"): torch.tensor([[0], [0]]),
    }

    metapaths = [[("author", "writes", "paper"), ("paper", "written_by", "author")]]

    # We need to manually specify num_nodes_dict because Author 1 is isolated
    # and won't appear in edge_index if we just infer from max index.
    num_nodes_dict = {"author": 2, "paper": 1}

    model = CustomMetaPath2Vec(
        edge_index_dict=edge_index_dict,
        embedding_dim=16,
        metapaths=metapaths,
        walk_length=2,
        context_size=2,
        walks_per_node=1,
        num_nodes_dict=num_nodes_dict,
    )

    # Batch containing both Author 0 and Author 1
    batch = torch.tensor([0, 1])

    # _pos_sample should return walks for Author 0, but prune walks for Author 1
    pos_rw = model._pos_sample(batch)

    # Check that no dummy nodes are present in the result
    # The dummy index is the total number of nodes (2 authors + 1 paper = 3)
    assert model.dummy_idx == 3
    assert (pos_rw < model.dummy_idx).all()

    # We expect fewer walks than input batch size because Author 1 should be pruned
    # Input batch: 2 nodes * 1 walk_per_node = 2 potential walks
    # Walk length 2. Nodes in walk: 3 (Start -> Mid -> End).
    # Context size 2.
    # Windows per walk: 1 + walk_length + 1 - context_size = 1 + 2 + 1 - 2 = 2.
    # Expected rows: 1 valid walk * 2 windows = 2 rows.
    assert pos_rw.size(0) == 2
