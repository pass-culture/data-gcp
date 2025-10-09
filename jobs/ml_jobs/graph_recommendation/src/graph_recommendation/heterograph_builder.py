"""Utilities to construct a PyTorch Geometric graph for book recommendations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import tqdm

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

import pandas as pd
import torch
from torch_geometric.data import HeteroData

ID_COLUMN = "item_id"
DEFAULT_METADATA_COLUMNS: Sequence[str] = (
    "gtl_label_level_1",
    "gtl_label_level_2",
    "gtl_label_level_3",
    "gtl_label_level_4",
    "artist_id",
)

MetadataKey = tuple[str, str]


def _normalise_value(value: object) -> str | None:
    """Normalize a single value to string or None.

    Args:
        value: The value to normalize.

    Returns:
        Normalized string value or None if empty/NaN.
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    result = str(value).strip()
    return result or None


def _normalize_dataframe(
    df: pd.DataFrame,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Normalize specified columns in dataframe using vectorized operations.

    Converts all values to strings, strips whitespace, and replaces empty/NaN
    values with None. This is more efficient than row-by-row normalization.

    Args:
        df: The input dataframe.
        columns: List of column names to normalize.

    Returns:
        A new dataframe with normalized columns.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            # Convert to string and strip whitespace
            df[col] = df[col].astype(str).str.strip()
            # Replace empty/missing value representations with None
            df[col] = df[col].replace(["", "nan", "None", "<NA>"], None)
    return df


def build_book_metadata_heterograph_from_dataframe(
    dataframe: pd.DataFrame,
    *,
    metadata_columns: Sequence[str],
    id_column: str,
) -> HeteroData:
    """Construct a heterogeneous book-to-metadata graph from a dataframe.

    The graph structure creates:
    - "book" node type for all books
    - One node type for each metadata column (e.g., "rayon", "artist_id")
    - "is_{metadata}" edge types from books to metadata
    - "{metadata}_of" edge types from metadata back to books

    Args:
        dataframe: Input data with book IDs and metadata columns.
        metadata_columns: Column names to use as metadata.
        id_column: Column name containing book IDs.

    Returns:
        A PyG HeteroData object with separate node and edge types.
    """
    missing_columns = [
        column
        for column in (id_column, *metadata_columns)
        if column not in dataframe.columns
    ]
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")

    # Step 1: Normalize all relevant columns using vectorized operations
    all_columns = [id_column, *metadata_columns]
    df_normalized = _normalize_dataframe(dataframe, all_columns)

    # Step 2: Prepare book nodes
    unique_books = df_normalized[id_column].dropna().drop_duplicates()
    book_ids = unique_books.tolist()
    book_index = {book_id: idx for idx, book_id in enumerate(book_ids)}

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
            # "book" -> "is_{column}" -> "{column}"
            edge_indices[("book", f"is_{column}", column)] = set()
            # "{column}" -> "{column}_of" -> "book"
            edge_indices[(column, f"{column}_of", "book")] = set()

    # Step 5: Build edges by iterating through dataframe
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

        book_idx = book_index[book_id]

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
                edge_key_forward = ("book", f"is_{column}", column)
                edge_key_backward = (column, f"{column}_of", "book")
                edge_indices[edge_key_forward].add((book_idx, metadata_idx))
                edge_indices[edge_key_backward].add((metadata_idx, book_idx))

    # Check if any edges were created
    total_edges = sum(len(edges) for edges in edge_indices.values())
    if total_edges == 0:
        raise ValueError(
            "No edges were created; check that metadata columns contain values."
        )

    # Step 6: Create HeteroData object
    graph_data = HeteroData()

    # Add book nodes
    graph_data["book"].num_nodes = len(book_ids)

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
    graph_data.book_ids = list(book_ids)
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


def build_book_metadata_heterograph(
    parquet_path: Path | str,
    *,
    nrows: int | None = None,
    filters: Sequence[tuple[str, str, Iterable[object]]] | None = None,
) -> HeteroData:
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

    return build_book_metadata_heterograph_from_dataframe(
        df, id_column=ID_COLUMN, metadata_columns=DEFAULT_METADATA_COLUMNS
    )
