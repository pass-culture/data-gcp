from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest
from torch_geometric.data import Data

from src.graph_builder import (
    DEFAULT_METADATA_COLUMNS,
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
            "gtl_id": ["01022000", "01030000", "01022000"],
        }
    )


def test_build_graph_from_dataframe_creates_bipartite_structure() -> None:
    dataframe = _build_sample_dataframe()

    graph_data = build_book_metadata_graph_from_dataframe(
        dataframe,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
        id_column="item_id",
    )

    assert isinstance(graph_data, Data)
    assert len(graph_data.book_ids) == 2
    assert len(graph_data.metadata_ids) == graph_data.num_nodes - len(
        graph_data.book_ids
    )
    assert graph_data.edge_index.shape[0] == 2

    # ensure undirected edges are mirrored
    edge_pairs = list(
        zip(
            graph_data.edge_index[0].tolist(),
            graph_data.edge_index[1].tolist(),
            strict=True,
        )
    )
    counter = Counter(edge_pairs)
    for source, target in edge_pairs:
        if source < len(graph_data.book_ids):
            assert counter[(source, target)] == 1
            assert counter[(target, source)] == 1

    # verify node types and masks
    assert graph_data.book_mask.sum().item() == len(graph_data.book_ids)
    assert graph_data.metadata_mask.sum().item() == len(graph_data.metadata_ids)
    assert graph_data.node_type[: len(graph_data.book_ids)].tolist() == [0] * len(
        graph_data.book_ids
    )
    assert all(
        value >= 1
        for value in graph_data.node_type[len(graph_data.book_ids) :].tolist()
    )


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
