from __future__ import annotations

import pandas as pd
import pytest
import torch
from torch_geometric.data import HeteroData

from src.heterograph_builder import (
    DEFAULT_METADATA_COLUMNS,
    build_book_metadata_heterograph_from_dataframe,
)


def _build_sample_dataframe() -> pd.DataFrame:
    """Build a sample dataframe for testing.
    Note: Metadata values must appear at least twice to survive preprocessing
    (detach_single_occuring_metadata removes singleton values).
    """
    return pd.DataFrame(
        {
            "item_id": ["book-1", "book-2", "book-3", "book-4"],
            "rayon": ["Literature", "Comics", "Literature", "Comics"],
            "gtl_label_level_1": ["Arts", "Arts", "Arts", "Arts"],
            "gtl_label_level_2": ["Painting", "Drawing", "Painting", "Drawing"],
            "gtl_label_level_3": ["Watercolor", "Sketching", "Watercolor", "Sketching"],
            "gtl_label_level_4": [None, None, None, None],
            "artist_id": ["artist-1", "artist-2", "artist-1", "artist-2"],
            "gtl_id": ["01022000", "01030000", "01022000", "01023000"],
        }
    )


def _build_empty_metadata_dataframe() -> pd.DataFrame:
    """Build a dataframe with no valid metadata values."""
    return pd.DataFrame(
        {
            "item_id": ["book-1", "book-2"],
            "rayon": [None, ""],
            "gtl_label_level_1": [None, "   "],
            "gtl_label_level_2": [None, None],
            "gtl_label_level_3": [None, None],
            "gtl_label_level_4": [None, None],
            "artist_id": [None, None],
            "gtl_id": ["01234567", "02345678"],
        }
    )


def test_build_heterograph_from_dataframe_creates_correct_structure() -> None:
    """Test that heterograph creates proper node and edge types."""
    dataframe = _build_sample_dataframe()
    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    assert isinstance(graph_data, HeteroData)
    # Check that we have the expected node types
    assert "book" in graph_data.node_types
    expected_metadata_types = {
        "gtl_label_level_1",
        "gtl_label_level_2",
        "gtl_label_level_3",
        "artist_id",
    }
    for node_type in expected_metadata_types:
        assert node_type in graph_data.node_types
    # gtl_label_level_4 should not be present since all values are None
    assert "gtl_label_level_4" not in graph_data.node_types
    # Check book nodes
    assert graph_data["book"].num_nodes == 4
    assert len(graph_data.book_ids) == 4
    assert set(graph_data.book_ids) == {"book-1", "book-2", "book-3", "book-4"}


def test_heterograph_edge_types_are_correct() -> None:
    """Test that edge types follow the expected naming convention."""
    dataframe = _build_sample_dataframe()
    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # Check that edge types exist for each metadata column
    expected_edge_patterns = [
        ("book", "is_gtl_label_level_1", "gtl_label_level_1"),
        ("gtl_label_level_1", "gtl_label_level_1_of", "book"),
        ("book", "is_gtl_label_level_2", "gtl_label_level_2"),
        ("gtl_label_level_2", "gtl_label_level_2_of", "book"),
        ("book", "is_gtl_label_level_3", "gtl_label_level_3"),
        ("gtl_label_level_3", "gtl_label_level_3_of", "book"),
        ("book", "is_artist_id", "artist_id"),
        ("artist_id", "artist_id_of", "book"),
    ]
    for src_type, edge_type, dst_type in expected_edge_patterns:
        assert (src_type, edge_type, dst_type) in graph_data.edge_types


def test_heterograph_edges_are_bidirectional() -> None:
    """Test that edges exist in both directions (book->metadata and metadata->book)."""
    dataframe = _build_sample_dataframe()
    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # For each forward edge, check that reverse edge exists
    for src_type, edge_type, dst_type in graph_data.edge_types:
        if src_type == "book" and edge_type.startswith("is_"):
            # Forward edge: book -> metadata
            forward_edges = graph_data[src_type, edge_type, dst_type].edge_index
            # Find corresponding reverse edge
            reverse_edge_type = f"{dst_type}_of"
            reverse_edges = graph_data[dst_type, reverse_edge_type, "book"].edge_index
            # Check that every forward edge has a corresponding reverse edge
            forward_pairs = set(
                zip(forward_edges[0].tolist(), forward_edges[1].tolist(), strict=True)
            )
            reverse_pairs = set(
                zip(reverse_edges[1].tolist(), reverse_edges[0].tolist(), strict=True)
            )
            assert (
                forward_pairs == reverse_pairs
            ), f"Edges not symmetric for {edge_type}"


def test_heterograph_metadata_organization() -> None:
    """Test that metadata is properly organized by column."""
    dataframe = _build_sample_dataframe()
    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # Check metadata_ids_by_column structure
    assert isinstance(graph_data.metadata_ids_by_column, dict)
    # Check that we have the right metadata columns
    expected_columns = {
        "gtl_label_level_1",
        "gtl_label_level_2",
        "gtl_label_level_3",
        "artist_id",
    }
    assert set(graph_data.metadata_columns) == expected_columns
    # Check specific metadata values
    assert "Arts" in graph_data.metadata_ids_by_column["gtl_label_level_1"]
    assert "Painting" in graph_data.metadata_ids_by_column["gtl_label_level_2"]
    assert "Drawing" in graph_data.metadata_ids_by_column["gtl_label_level_2"]
    assert "Watercolor" in graph_data.metadata_ids_by_column["gtl_label_level_3"]
    assert "Sketching" in graph_data.metadata_ids_by_column["gtl_label_level_3"]
    assert "artist-1" in graph_data.metadata_ids_by_column["artist_id"]
    assert "artist-2" in graph_data.metadata_ids_by_column["artist_id"]
    # Check that flattened metadata_ids exists for backward compatibility
    assert hasattr(graph_data, "metadata_ids")
    assert isinstance(graph_data.metadata_ids, list)
    assert len(graph_data.metadata_ids) > 0
    # Check that metadata_ids contains tuples of (column, value)
    for metadata_id in graph_data.metadata_ids:
        assert isinstance(metadata_id, tuple)
        assert len(metadata_id) == 2
        column, value = metadata_id
        assert column in graph_data.metadata_columns
        assert value in graph_data.metadata_ids_by_column[column]


def test_heterograph_node_counts_are_correct() -> None:
    """Test that node counts match the expected unique values."""
    dataframe = _build_sample_dataframe()
    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # Check book node count
    assert graph_data["book"].num_nodes == 4
    # Check metadata node counts
    assert graph_data["gtl_label_level_1"].num_nodes == 1  # Only "Arts"
    assert graph_data["gtl_label_level_2"].num_nodes == 2  # "Painting", "Drawing"
    assert graph_data["gtl_label_level_3"].num_nodes == 2  # "Watercolor", "Sketching"
    assert graph_data["artist_id"].num_nodes == 2  # "artist-1", "artist-2"


def test_heterograph_missing_columns_raises_error() -> None:
    """Test that missing required columns raise KeyError."""
    dataframe = pd.DataFrame({"item_id": ["book-1"], "some_other_column": ["value"]})
    with pytest.raises(KeyError, match="Missing required columns"):
        build_book_metadata_heterograph_from_dataframe(
            dataframe,
            metadata_columns=["missing_column"],
            id_column="item_id",
            gtl_id_column="gtl_id",
        )


def test_heterograph_no_valid_metadata_raises_error() -> None:
    """Test that dataframe with no valid metadata raises ValueError."""
    dataframe = _build_empty_metadata_dataframe()
    with pytest.raises(ValueError, match="No edges were created"):
        build_book_metadata_heterograph_from_dataframe(
            dataframe,
            metadata_columns=DEFAULT_METADATA_COLUMNS,
            id_column="item_id",
            gtl_id_column="gtl_id",
        )


def test_heterograph_handles_duplicate_books() -> None:
    """Test that duplicate books are handled correctly."""
    dataframe = pd.DataFrame(
        {
            "item_id": ["book-1", "book-1", "book-2", "book-3", "book-4"],
            "artist_id": ["artist-1", "artist-1", "artist-2", "artist-1", "artist-2"],
            "gtl_label_level_1": ["Arts", "Arts", "Comics", "Arts", "Comics"],
            "gtl_id": ["01234567", "01234567", "02345678", "03456789", "04567890"],
        }
    )

    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=["artist_id", "gtl_label_level_1"],
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # Should have unique books only (book-1 appears twice but deduplicated)
    # All metadata values appear at least twice, so they survive preprocessing
    assert graph_data["book"].num_nodes == 4
    assert set(graph_data.book_ids) == {"book-1", "book-2", "book-3", "book-4"}


def test_heterograph_edge_indices_are_valid_tensors() -> None:
    """Test that edge indices are valid PyTorch tensors with correct format."""
    dataframe = _build_sample_dataframe()
    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    for src_type, edge_type, dst_type in graph_data.edge_types:
        edge_index = graph_data[src_type, edge_type, dst_type].edge_index
        # Check tensor properties
        assert isinstance(edge_index, torch.Tensor)
        assert edge_index.dtype == torch.long
        assert edge_index.dim() == 2
        assert edge_index.size(0) == 2  # Should be 2 x num_edges
        assert edge_index.size(1) > 0  # Should have at least one edge
        # Check that indices are within valid ranges
        src_indices = edge_index[0]
        dst_indices = edge_index[1]
        assert src_indices.min() >= 0
        assert dst_indices.min() >= 0
        if src_type == "book":
            assert src_indices.max() < graph_data["book"].num_nodes
        else:
            assert src_indices.max() < graph_data[src_type].num_nodes
        if dst_type == "book":
            assert dst_indices.max() < graph_data["book"].num_nodes
        else:
            assert dst_indices.max() < graph_data[dst_type].num_nodes


def test_heterograph_handles_mixed_data_types() -> None:
    """Test that the heterograph builder handles mixed data types correctly."""
    dataframe = pd.DataFrame(
        {
            "item_id": ["book-1", "book-2", "book-3", "book-4"],
            # Mixed types, 123 appears twice
            "artist_id": [123, "artist-string", 456.789, 123],
            # Category A appears twice
            "gtl_label_level_1": ["Category A", 999, None, "Category A"],
            "gtl_id": ["01234567", "02345678", "gtl-3", "gtl-4"],
        }
    )

    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=["artist_id", "gtl_label_level_1"],
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # Check that mixed types are converted to strings
    # Only values appearing at least twice survive preprocessing
    artist_values = graph_data.metadata_ids_by_column["artist_id"]
    assert "123" in artist_values  # Appears twice
    # "artist-string" and "456.789" appear only once, so they're removed
    gtl_values = graph_data.metadata_ids_by_column["gtl_label_level_1"]
    assert "Category A" in gtl_values  # Appears twice
    # "999" appears only once, so it's removed
    # None should be excluded


def test_heterograph_with_single_metadata_column() -> None:
    """Test heterograph creation with only one metadata column."""
    dataframe = pd.DataFrame(
        {
            "item_id": ["book-1", "book-2", "book-3", "book-4", "book-5"],
            "artist_id": ["artist-1", "artist-2", "artist-1", "artist-2", None],
            "gtl_id": ["01234567", "02345678", "03456789", "gtl-4", "gtl-5"],
        }
    )

    graph_data = build_book_metadata_heterograph_from_dataframe(
        dataframe,
        metadata_columns=["artist_id"],
        id_column="item_id",
        gtl_id_column="gtl_id",
    )
    # Should have book and artist_id node types
    assert set(graph_data.node_types) == {"book", "artist_id"}
    # Should have exactly 2 edge types (forward and backward)
    expected_edge_types = {
        ("book", "is_artist_id", "artist_id"),
        ("artist_id", "artist_id_of", "book"),
    }
    assert set(graph_data.edge_types) == expected_edge_types

    # Check node counts
    assert (
        graph_data["book"].num_nodes == 4
    )  # book-5 has no artist_id nor proper gtl_id
    assert graph_data["artist_id"].num_nodes == 2
