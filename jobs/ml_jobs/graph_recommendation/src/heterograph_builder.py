"""Utilities to construct a PyTorch Geometric graph for item recommendations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.constants import DEFAULT_METADATA_COLUMNS, GTL_ID_COLUMN, ID_COLUMN
from src.utils.preprocessing import preprocess_metadata_dataframe

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from src.constants import MetadataKey

import pandas as pd
import torch
from torch_geometric.data import HeteroData


def build_item_metadata_heterograph_from_dataframe(
    dataframe: pd.DataFrame,
    metadata_columns: Sequence[str],
) -> HeteroData:
    """Construct a heterogeneous item-to-metadata graph from a dataframe.

    The graph structure creates:
    - "item" node type for all items
    - One node type for each metadata column (e.g., "rayon", "artist_id")
    - "is_{metadata}" edge types from items to metadata
    - "{metadata}_of" edge types from metadata back to items

    Args:
        dataframe: Input data with item IDs and metadata columns.
        metadata_columns: Column names to use as metadata.
        id_column: Column name containing item IDs.
        gtl_id_column: Column name containing GTL IDs.

    Returns:
        A PyG HeteroData object with separate node and edge types.
    TODO: Refactor to treat gtl_id as a metadata column (or remove it entirely).
    """
    missing_columns = [
        column
        for column in (ID_COLUMN, GTL_ID_COLUMN, *metadata_columns)
        if column not in dataframe.columns
    ]
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")

    # Step 1: Preprocess dataframe
    df_normalized = preprocess_metadata_dataframe(
        dataframe,
        metadata_columns=[GTL_ID_COLUMN, *metadata_columns],
    )

    # Step 2: Prepare item nodes
    unique_items = df_normalized[[ID_COLUMN, GTL_ID_COLUMN]].drop_duplicates(
        subset=[ID_COLUMN]
    )
    item_ids = unique_items[ID_COLUMN].tolist()
    gtl_ids = unique_items[GTL_ID_COLUMN].tolist()
    item_index = {item_id: idx for idx, item_id in enumerate(item_ids)}

    # Step 3: Build metadata nodes by column
    metadata_nodes_by_column: dict[str, dict[str, int]] = {}
    metadata_ids_by_column: dict[str, list[str]] = {}

    for column in metadata_columns:
        if column not in df_normalized.columns:
            continue

        unique_values = df_normalized[column].dropna().unique()
        valid_values = [
            str(value)
            for value in unique_values
            if value is not None and str(value).strip() != "None"
        ]

        if valid_values:
            metadata_nodes_by_column[column] = {
                value: idx for idx, value in enumerate(valid_values)
            }
            metadata_ids_by_column[column] = valid_values

    # Step 4: Create edge indices for each relation type
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]] = {}

    # Initialize edge sets for each metadata column
    for column in metadata_columns:
        if column in metadata_nodes_by_column:
            # "item" -> "is_{column}" -> "{column}"
            edge_indices[("item", f"is_{column}", column)] = set()
            # "{column}" -> "{column}_of" -> "item"
            edge_indices[(column, f"{column}_of", "item")] = set()

    # Step 5: Build edges by iterating through dataframe
    relevant_columns = [ID_COLUMN, *metadata_columns]

    for record in df_normalized[relevant_columns].itertuples(index=False):
        record_dict = record._asdict()
        item_id = record_dict[ID_COLUMN]
        # Skip rows with missing item IDs
        if item_id is None or item_id not in item_index:
            continue

        item_idx = item_index[item_id]

        # Create edges to all metadata values in this row
        for column in metadata_columns:
            if column not in metadata_nodes_by_column:
                continue

            value = record_dict[column]
            if value is None or str(value).strip() == "None":
                continue

            value_str = str(value)
            if value_str in metadata_nodes_by_column[column]:
                metadata_idx = metadata_nodes_by_column[column][value_str]

                # Add edges in both directions
                edge_key_forward = ("item", f"is_{column}", column)
                edge_key_backward = (column, f"{column}_of", "item")
                edge_indices[edge_key_forward].add((item_idx, metadata_idx))
                edge_indices[edge_key_backward].add((metadata_idx, item_idx))

    # Check if any edges were created
    total_edges = sum(len(edges) for edges in edge_indices.values())
    if total_edges == 0:
        raise ValueError(
            "No edges were created; check that metadata columns contain values."
        )

    # Step 6: Create HeteroData object
    graph_data = HeteroData()

    # Add item nodes
    graph_data["item"].num_nodes = len(item_ids)

    # Add metadata nodes for each column
    for column, node_mapping in metadata_nodes_by_column.items():
        graph_data[column].num_nodes = len(node_mapping)

    # Add edge indices
    for (src_type, edge_type, dst_type), edges in edge_indices.items():
        if edges:  # Only add if there are actual edges
            sorted_edges = sorted(edges)
            edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()
            graph_data[src_type, edge_type, dst_type].edge_index = edge_index

    # Step 7: Add custom attributes for identifier mapping
    graph_data.item_ids = list(item_ids)
    graph_data.gtl_ids = list(gtl_ids)
    graph_data.metadata_ids_by_column = metadata_ids_by_column
    graph_data.metadata_columns = [
        col for col in metadata_columns if col in metadata_nodes_by_column
    ]

    # Create a flattened metadata_ids list for backward compatibility
    metadata_keys: list[MetadataKey] = []
    for column in graph_data.metadata_columns:
        for value in metadata_ids_by_column[column]:
            metadata_keys.append((column, value))
    graph_data.metadata_ids = metadata_keys

    return graph_data


def build_item_metadata_heterograph(
    parquet_path: str,
    *,
    nrows: int | None = None,
    filters: Sequence[tuple[str, str, Iterable[object]]] | None = None,
) -> HeteroData:
    """Load a parquet file and build the corresponding item-metadata graph."""

    read_kwargs: dict[str, object] = {}
    if filters is not None:
        read_kwargs["filters"] = list(filters)

    df = pd.read_parquet(parquet_path, **read_kwargs)
    if nrows is not None:
        df = df.sample(min(len(df), nrows), random_state=42)

    data_graph = build_item_metadata_heterograph_from_dataframe(
        df,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
    )

    return data_graph
