"""Utilities to construct a PyTorch Geometric graph for book recommendations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import tqdm

from src.constants import DEFAULT_METADATA_COLUMNS, ID_COLUMN
from src.utils.graph_indexing import build_id_to_index_map
from src.utils.postprocessing import (
    diagnose_component_sizes,
)
from src.utils.preprocessing import normalize_dataframe, remove_rows_with_no_metadata

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from src.constants import MetadataKey

import pandas as pd
import torch
from loguru import logger
from torch_geometric.data import Data


def _build_metadata_index(
    df: pd.DataFrame,
    metadata_columns: Sequence[str],
    metadata_type_to_id: dict[str, int],
    num_books: int,
) -> tuple[list[MetadataKey], dict[MetadataKey, int], list[int]]:
    """Build index of all unique metadata nodes before edge creation.

    This separates metadata discovery from edge creation, making the code
    clearer and avoiding incremental list/dict updates in the hot loop.

    Args:
        df: The normalized dataframe.
        metadata_columns: List of metadata column names.
        metadata_type_to_id: Mapping from column names to type IDs.
        num_books: Number of book nodes (metadata indices start after this).

    Returns:
        A tuple of:
        - metadata_keys: Ordered list of (column, value) tuples
        - metadata_index: Mapping from keys to node indices
        - node_types: List of type IDs for each metadata node
    """
    metadata_keys: list[MetadataKey] = []
    metadata_index: dict[MetadataKey, int] = {}
    node_types: list[int] = []

    # Collect all unique (column, value) pairs
    for column in metadata_columns:
        if column not in df.columns:
            continue

        unique_values = df[column].dropna().unique()
        for value in unique_values:
            # Skip None values that survived normalization
            if value is None or value == "None":
                continue

            key = (column, str(value))
            if key not in metadata_index:
                # Metadata nodes are indexed after book nodes
                idx = num_books + len(metadata_keys)
                metadata_index[key] = idx
                metadata_keys.append(key)
                node_types.append(metadata_type_to_id[column])

    return metadata_keys, metadata_index, node_types


def build_book_metadata_graph_from_dataframe(
    dataframe: pd.DataFrame,
    *,
    metadata_columns: Sequence[str],
    id_column: str,
) -> Data:
    """Construct a bipartite book-to-metadata graph from a dataframe.

    The graph structure is:
    - Book nodes are indexed 0 to (num_books - 1)
    - Metadata nodes are indexed num_books to (num_total_nodes - 1)
    - Edges are bidirectional (undirected graph)

    Args:
        dataframe: Input data with book IDs and metadata columns.
        metadata_columns: Column names to use as metadata.
        id_column: Column name containing book IDs.

    Returns:
        A PyG Data object with edge_index and custom attributes for mapping
        embeddings back to identifiers.
    """
    missing_columns = [
        column
        for column in (id_column, *metadata_columns)
        if column not in dataframe.columns
    ]
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")

    # Step 1: Preprocessing
    all_columns = [id_column, *metadata_columns]
    df_normalized = normalize_dataframe(dataframe, all_columns)
    df_normalized = remove_rows_with_no_metadata(
        df_normalized,
        metadata_list=list(metadata_columns),
    )

    # Step 2: Prepare book nodes (indexed 0 to num_books - 1)
    unique_books = df_normalized[id_column].dropna().drop_duplicates()
    book_ids = unique_books.tolist()
    book_index = build_id_to_index_map(book_ids)

    # Step 3: Prepare metadata type mapping (0 reserved for books)
    metadata_type_to_id = {"book": 0}
    for offset, column_name in enumerate(metadata_columns, start=1):
        metadata_type_to_id[column_name] = offset

    # Step 4: Build metadata index (discovers all unique metadata nodes)
    metadata_keys, metadata_index, metadata_node_types = _build_metadata_index(
        df_normalized,
        metadata_columns,
        metadata_type_to_id,
        num_books=len(book_ids),
    )

    # Step 5: Create node type list (books first, then metadata)
    node_types = [metadata_type_to_id["book"]] * len(book_ids) + metadata_node_types

    # Step 6: Create edges by iterating through rows
    edges: set[tuple[int, int]] = set()
    relevant_columns = [id_column, *metadata_columns]

    for record in tqdm.tqdm(
        df_normalized[relevant_columns].itertuples(index=False),
        desc="Building edges",
    ):
        record_dict = record._asdict()
        book_id = record_dict[id_column]

        # Skip rows with missing book IDs
        if book_id is None or book_id not in book_index:
            continue

        source_idx = book_index[book_id]

        # Create edges to all metadata values in this row
        for column in metadata_columns:
            value = record_dict[column]
            if value is None or value == "None":
                continue

            key = (column, value)
            target_idx = metadata_index.get(key)
            if target_idx is None:
                continue

            # Add bidirectional edges (undirected graph)
            edge = (source_idx, target_idx)
            if edge not in edges:
                edges.add(edge)
                edges.add((target_idx, source_idx))

    if not edges:
        raise ValueError(
            "No edges were created; check that metadata columns contain values."
        )

    # Step 7: Create PyG Data object with edge_index tensor
    sorted_edges = sorted(edges)
    edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()
    graph_data = Data(edge_index=edge_index, num_nodes=len(node_types))

    # Step 8: Add custom attributes for identifier mapping
    # Note: This is a homogeneous graph (Data), not heterogeneous (HeteroData),
    # so we manually set attributes instead of using set_graph_identifiers
    graph_data.node_type = torch.tensor(node_types, dtype=torch.long)
    book_mask = torch.zeros(len(node_types), dtype=torch.bool)
    book_mask[: len(book_ids)] = True
    graph_data.book_mask = book_mask
    graph_data.metadata_mask = ~book_mask
    graph_data.metadata_type_to_id = metadata_type_to_id
    graph_data.metadata_columns = list(metadata_columns)
    graph_data.book_ids = list(book_ids)
    graph_data.metadata_ids = list(metadata_keys)
    graph_data.node_ids = graph_data.book_ids + graph_data.metadata_ids

    return graph_data


def build_book_metadata_graph(
    parquet_path: Path | str,
    *,
    nrows: int | None = None,
    filters: Sequence[tuple[str, str, Iterable[object]]] | None = None,
) -> Data:
    """Load a parquet file and build the corresponding book-metadata graph."""

    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    read_kwargs: dict[str, object] = {}
    if filters is not None:
        read_kwargs["filters"] = list(filters)

    df = pd.read_parquet(path, **read_kwargs)
    if nrows is not None:
        df = df.sample(nrows, random_state=42)

    data_graph = build_book_metadata_graph_from_dataframe(
        df, id_column=ID_COLUMN, metadata_columns=DEFAULT_METADATA_COLUMNS
    )

    _ = diagnose_component_sizes(graph=data_graph)

    logger.warning("Pruning not available on homogeneous Graph!")

    return data_graph
