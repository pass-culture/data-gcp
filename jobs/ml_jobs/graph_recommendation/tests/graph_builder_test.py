from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest

from src.graph_recommendation.graph_builder import (
    DEFAULT_METADATA_COLUMNS,
    BookMetadataGraph,
    build_book_metadata_graph_from_dataframe,
)


def _build_sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "item_id": ["book-1", "book-2", "book-1"],
            "rayon": ["Literature", "Comics", "Literature"],
            "gtl_label_level_1": ["Arts", "Arts", "Arts"],
            "gtl_label_level_2": ["Painting", "Drawing", "Painting"],
            "gtl_label_level_3": ["Watercolor", None, "Watercolor"],
            "gtl_label_level_4": [None, None, None],
            "artist_id": ["artist-1", "artist-2", "artist-1"],
        }
    )


def test_build_graph_from_dataframe_creates_bipartite_structure() -> None:
    dataframe = _build_sample_dataframe()

    graph = build_book_metadata_graph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
    )
    data = graph.data

    assert isinstance(graph, BookMetadataGraph)
    assert len(graph.book_ids) == 2
    assert len(graph.metadata_keys) == data.num_nodes - len(graph.book_ids)
    assert data.edge_index.shape[0] == 2

    # ensure undirected edges are mirrored
    edge_pairs = list(
        zip(
            data.edge_index[0].tolist(),
            data.edge_index[1].tolist(),
            strict=True,
        )
    )
    counter = Counter(edge_pairs)
    for source, target in edge_pairs:
        if source < len(graph.book_ids):
            assert counter[(source, target)] == 1
            assert counter[(target, source)] == 1

    # verify node types and masks
    assert data.book_mask.sum().item() == len(graph.book_ids)
    assert data.metadata_mask.sum().item() == len(graph.metadata_keys)
    assert data.node_type[: len(graph.book_ids)].tolist() == [0] * len(graph.book_ids)
    assert all(value >= 1 for value in data.node_type[len(graph.book_ids) :].tolist())

    # identifier mappings are stored on both the graph container and the PyG data object
    assert graph.book_id_to_index == {"book-1": 0, "book-2": 1}
    assert set(graph.metadata_key_to_index) == set(graph.metadata_keys)
    assert data.book_ids == graph.book_ids
    assert data.metadata_keys == graph.metadata_keys
    assert data.book_id_to_index == graph.book_id_to_index
    assert data.metadata_key_to_index == graph.metadata_key_to_index


def test_missing_metadata_raises_value_error() -> None:
    dataframe = pd.DataFrame(
        {
            "item_id": ["book-1"],
            "rayon": [None],
            "gtl_label_level_1": [None],
            "gtl_label_level_2": [None],
            "gtl_label_level_3": [None],
            "gtl_label_level_4": [None],
            "artist_id": [None],
        }
    )

    with pytest.raises(ValueError, match="No edges were created"):
        build_book_metadata_graph_from_dataframe(
            dataframe,
            metadata_columns=DEFAULT_METADATA_COLUMNS,
            id_column="item_id",
        )
