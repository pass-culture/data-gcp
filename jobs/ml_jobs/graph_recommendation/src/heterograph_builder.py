"""Utilities to construct a PyTorch Geometric graph for book/music recommendations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.constants import (
    DEFAULT_METADATA_COLUMNS,
    GTL_ID_COLUMN,
    GTL_METADATA_COLUMNS,
    GTL_METADATA_COLUMNS_BY_ITEM_TYPE,
    ID_COLUMN,
    ITEM_TYPE_COLUMN,
    KNOWN_ITEM_TYPES,
    SHARED_METADATA_COLUMNS,
)
from src.utils.preprocessing import preprocess_metadata_dataframe

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from src.constants import MetadataKey

import pandas as pd
import torch
from torch_geometric.data import HeteroData


def _gtl_node_type(item_type: str, column: str) -> str:
    """Return the namespaced GTL node-type for a given item type.

    GTL labels carry item-type-specific semantics (a GTL code for books has no
    relation to the same code for music), so we namespace them to avoid creating
    spurious cross-type edges.

    Example:
        >>> _gtl_node_type("music", "gtl_label_level_1")
        'music_gtl_label_level_1'
    """
    return f"{item_type}_{column}"


def build_book_metadata_heterograph_from_dataframe(
    dataframe: pd.DataFrame,
    metadata_columns: Sequence[str],
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

    # Route to the multi-type builder when an item_type column is present
    if ITEM_TYPE_COLUMN in dataframe.columns:
        return build_multitype_metadata_heterograph_from_dataframe(
            dataframe,
            gtl_metadata_columns=list(GTL_METADATA_COLUMNS),
            shared_metadata_columns=list(SHARED_METADATA_COLUMNS),
        )

    # --- legacy single-type (book-only) path ---

    # Step 1: Preprocess dataframe
    df_normalized = preprocess_metadata_dataframe(
        dataframe,
        metadata_columns=[GTL_ID_COLUMN, *metadata_columns],
    )

    # Step 2: Prepare book nodes
    unique_books = df_normalized[[ID_COLUMN, GTL_ID_COLUMN]].drop_duplicates(
        subset=[ID_COLUMN]
    )
    book_ids = unique_books[ID_COLUMN].tolist()
    gtl_ids = unique_books[GTL_ID_COLUMN].tolist()
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
            edge_indices[("book", f"is_{column}", column)] = set()
            edge_indices[(column, f"{column}_of", "book")] = set()

    # Step 5: Build edges by iterating through dataframe
    relevant_columns = [ID_COLUMN, *metadata_columns]

    for record in df_normalized[relevant_columns].itertuples(index=False):
        record_dict = record._asdict()
        book_id = record_dict[ID_COLUMN]
        if book_id is None or book_id not in book_index:
            continue

        book_idx = book_index[book_id]

        for column in metadata_columns:
            if column not in metadata_nodes_by_column:
                continue

            value = record_dict[column]
            if value is None or str(value).strip() == "None":
                continue

            value_str = str(value)
            if value_str in metadata_nodes_by_column[column]:
                metadata_idx = metadata_nodes_by_column[column][value_str]
                edge_indices[("book", f"is_{column}", column)].add(
                    (book_idx, metadata_idx)
                )
                edge_indices[(column, f"{column}_of", "book")].add(
                    (metadata_idx, book_idx)
                )

    # Check if any edges were created
    total_edges = sum(len(edges) for edges in edge_indices.values())
    if total_edges == 0:
        raise ValueError(
            "No edges were created; check that metadata columns contain values."
        )

    # Step 6: Create HeteroData object
    graph_data = HeteroData()
    graph_data["book"].num_nodes = len(book_ids)

    for column, node_mapping in metadata_nodes_by_column.items():
        graph_data[column].num_nodes = len(node_mapping)

    for (src_type, edge_type, dst_type), edges in edge_indices.items():
        if edges:
            sorted_edges = sorted(edges)
            edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()
            graph_data[src_type, edge_type, dst_type].edge_index = edge_index

    # Step 7: Add custom attributes for identifier mapping
    graph_data.book_ids = list(book_ids)
    graph_data.gtl_ids = list(gtl_ids)
    graph_data.metadata_ids_by_column = metadata_ids_by_column
    graph_data.metadata_columns = [
        col for col in metadata_columns if col in metadata_nodes_by_column
    ]

    metadata_keys: list[MetadataKey] = []
    for column in graph_data.metadata_columns:
        for value in metadata_ids_by_column[column]:
            metadata_keys.append((column, value))
    graph_data.metadata_ids = metadata_keys
    # Multi-type attributes (empty for backward-compat)
    graph_data.item_ids_by_type = {"book": list(book_ids)}
    graph_data.gtl_ids_by_type = {"book": list(gtl_ids)}

    return graph_data


# ---------------------------------------------------------------------------
# Multi-type builder
# ---------------------------------------------------------------------------


def build_multitype_metadata_heterograph_from_dataframe(
    dataframe: pd.DataFrame,
    gtl_metadata_columns: list[str],
    shared_metadata_columns: list[str],
) -> HeteroData:
    """Construct a heterogeneous graph supporting multiple item types.

    All items (books, music, …) are stored under a **single node type
    ``"item"``** so that MetaPath2Vec can start random walks from a unique
    node type.  The original ``item_type`` value is preserved as a per-node
    attribute (``graph_data.item_types``) and in the ``item_ids_by_type`` /
    ``gtl_ids_by_type`` mappings so that embeddings can be split back by type
    after training.

    **Node types**
    - ``"item"`` — all items regardless of type (books, music, …).
    - One node type per *shared* metadata column (e.g. ``artist_id``,
      ``series_id``): these nodes bridge different item types — for instance,
      a book and a CD by the same artist are connected through the same
      ``artist_id`` node.
    - One namespaced node type per *(item_type, gtl_column)* pair (e.g.
      ``"book_gtl_label_level_1"``, ``"music_gtl_label_level_1"``): GTL codes
      are item-type-specific — the same numeric code for books and music
      carries unrelated semantic meaning, so they must NOT be shared.

    **Edge types** (bipartite, both directions)
    - ``("item", "is_{C}", node_type)``  — item → metadata node
    - ``(node_type, "{C}_of", "item")``  — metadata node → item

    Args:
        dataframe: Input DataFrame. Must contain ``item_id``, ``gtl_id``,
            ``item_type``, and the union of *gtl_metadata_columns* and
            *shared_metadata_columns*.
        gtl_metadata_columns: Columns whose nodes are namespaced per item type
            (e.g. ``gtl_label_level_1``).
        shared_metadata_columns: Columns whose nodes are shared across all
            item types (e.g. ``artist_id``).

    Returns:
        A :class:`torch_geometric.data.HeteroData` object.
    """
    all_metadata_columns = [*gtl_metadata_columns, *shared_metadata_columns]

    required = [ID_COLUMN, GTL_ID_COLUMN, ITEM_TYPE_COLUMN, *all_metadata_columns]
    missing = [c for c in required if c not in dataframe.columns]
    if missing:
        raise KeyError(f"Missing required columns: {', '.join(missing)}")

    # --- 1. Normalise the full dataframe ---
    df_normalized = preprocess_metadata_dataframe(
        dataframe,
        metadata_columns=[GTL_ID_COLUMN, *all_metadata_columns],
    )

    # Normalise item_type column (strip / lower)
    df_normalized[ITEM_TYPE_COLUMN] = (
        df_normalized[ITEM_TYPE_COLUMN].astype(str).str.strip().str.lower()
    )

    item_types_present = [
        t for t in KNOWN_ITEM_TYPES if t in df_normalized[ITEM_TYPE_COLUMN].unique()
    ]
    unknown_types = set(df_normalized[ITEM_TYPE_COLUMN].unique()) - set(
        KNOWN_ITEM_TYPES
    )
    if unknown_types:
        import warnings

        warnings.warn(
            f"Unknown item_type values will be ignored: {unknown_types}",
            stacklevel=2,
        )

    # --- 2. Index ALL items under a single "item" node type ---
    # Items are sorted by item_type first for determinism, then by item_id.
    df_items = (
        df_normalized[[ID_COLUMN, GTL_ID_COLUMN, ITEM_TYPE_COLUMN]]
        .drop_duplicates(subset=[ID_COLUMN])
        .sort_values([ITEM_TYPE_COLUMN, ID_COLUMN])
        .reset_index(drop=True)
    )
    # Filter to known item types only
    df_items = df_items[df_items[ITEM_TYPE_COLUMN].isin(item_types_present)]

    all_item_ids: list[str] = df_items[ID_COLUMN].tolist()
    all_gtl_ids: list[str] = df_items[GTL_ID_COLUMN].tolist()
    all_item_types: list[str] = df_items[ITEM_TYPE_COLUMN].tolist()
    item_index: dict[str, int] = {iid: idx for idx, iid in enumerate(all_item_ids)}

    # Build per-type slices for post-training embedding extraction
    item_ids_by_type: dict[str, list[str]] = {}
    gtl_ids_by_type: dict[str, list[str]] = {}
    for item_type in item_types_present:
        mask = df_items[ITEM_TYPE_COLUMN] == item_type
        item_ids_by_type[item_type] = df_items.loc[mask, ID_COLUMN].tolist()
        gtl_ids_by_type[item_type] = df_items.loc[mask, GTL_ID_COLUMN].tolist()

    # --- 3. Build metadata node indices ---
    # 3a. Shared metadata: pool values across all item types
    shared_nodes: dict[str, dict[str, int]] = {}
    shared_ids: dict[str, list[str]] = {}
    for col in shared_metadata_columns:
        if col not in df_normalized.columns:
            continue
        unique_vals = [
            str(v)
            for v in df_normalized[col].dropna().unique()
            if v is not None and str(v).strip() not in ("None", "")
        ]
        if unique_vals:
            shared_nodes[col] = {v: i for i, v in enumerate(unique_vals)}
            shared_ids[col] = unique_vals

    # 3b. GTL metadata: separate node spaces per item type, using only the
    # GTL levels that are relevant for each item type (e.g. music has no
    # levels 3 & 4, so we never create useless empty node types for them).
    gtl_nodes: dict[str, dict[str, int]] = {}  # key = namespaced node type
    gtl_ids_map: dict[str, list[str]] = {}

    for item_type in item_types_present:
        df_type = df_normalized[df_normalized[ITEM_TYPE_COLUMN] == item_type]
        # Use the per-type GTL column list; fall back to the full list for
        # unknown item types so the builder stays future-proof.
        applicable_gtl_columns = GTL_METADATA_COLUMNS_BY_ITEM_TYPE.get(
            item_type, gtl_metadata_columns
        )
        for col in applicable_gtl_columns:
            if col not in df_type.columns:
                continue
            node_type = _gtl_node_type(item_type, col)
            unique_vals = [
                str(v)
                for v in df_type[col].dropna().unique()
                if v is not None and str(v).strip() not in ("None", "")
            ]
            if unique_vals:
                gtl_nodes[node_type] = {v: i for i, v in enumerate(unique_vals)}
                gtl_ids_map[node_type] = unique_vals

    # --- 4. Build edge index sets ---
    # All edges use "item" as the item-side node type regardless of item_type.
    # GTL metadata nodes are still namespaced per item_type (book_gtl_...,
    # music_gtl_...) to keep their semantics separate.
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]] = {}

    def _ensure_edge(key: tuple[str, str, str]) -> None:
        if key not in edge_indices:
            edge_indices[key] = set()

    # Precompute edge type keys
    for item_type in item_types_present:
        for col in shared_metadata_columns:
            if col in shared_nodes:
                _ensure_edge(("item", f"is_{col}", col))
                _ensure_edge((col, f"{col}_of", "item"))
        applicable_gtl_columns = GTL_METADATA_COLUMNS_BY_ITEM_TYPE.get(
            item_type, gtl_metadata_columns
        )
        for col in applicable_gtl_columns:
            node_type = _gtl_node_type(item_type, col)
            if node_type in gtl_nodes:
                _ensure_edge(("item", f"is_{col}_{item_type}", node_type))
                _ensure_edge((node_type, f"{col}_{item_type}_of", "item"))

    # Iterate rows and build edges
    all_edge_columns = [ID_COLUMN, ITEM_TYPE_COLUMN, *all_metadata_columns]
    for record in df_normalized[all_edge_columns].itertuples(index=False):
        record_dict = record._asdict()
        item_id = record_dict[ID_COLUMN]
        item_type = record_dict[ITEM_TYPE_COLUMN]

        if item_type not in item_types_present:
            continue
        if item_id not in item_index:
            continue

        item_idx = item_index[item_id]

        # Shared metadata edges (edge type independent of item_type)
        for col in shared_metadata_columns:
            if col not in shared_nodes:
                continue
            value = record_dict.get(col)
            if value is None or str(value).strip() in ("None", ""):
                continue
            value_str = str(value)
            if value_str in shared_nodes[col]:
                meta_idx = shared_nodes[col][value_str]
                edge_indices[("item", f"is_{col}", col)].add((item_idx, meta_idx))
                edge_indices[(col, f"{col}_of", "item")].add((meta_idx, item_idx))

        # GTL (namespaced) metadata edges — edge type includes item_type suffix
        # so that book GTL edges and music GTL edges remain distinct relation types
        applicable_gtl_columns = GTL_METADATA_COLUMNS_BY_ITEM_TYPE.get(
            item_type, gtl_metadata_columns
        )
        for col in applicable_gtl_columns:
            node_type = _gtl_node_type(item_type, col)
            if node_type not in gtl_nodes:
                continue
            value = record_dict.get(col)
            if value is None or str(value).strip() in ("None", ""):
                continue
            value_str = str(value)
            if value_str in gtl_nodes[node_type]:
                meta_idx = gtl_nodes[node_type][value_str]
                edge_indices[("item", f"is_{col}_{item_type}", node_type)].add(
                    (item_idx, meta_idx)
                )
                edge_indices[(node_type, f"{col}_{item_type}_of", "item")].add(
                    (meta_idx, item_idx)
                )

    # --- 5. Sanity check ---
    total_edges = sum(len(e) for e in edge_indices.values())
    if total_edges == 0:
        raise ValueError(
            "No edges were created; check that metadata columns contain values."
        )

    # --- 6. Assemble HeteroData ---
    graph_data = HeteroData()

    # Single "item" node type for all items
    graph_data["item"].num_nodes = len(all_item_ids)

    for col, node_mapping in shared_nodes.items():
        graph_data[col].num_nodes = len(node_mapping)

    for node_type, node_mapping in gtl_nodes.items():
        graph_data[node_type].num_nodes = len(node_mapping)

    for (src_type, edge_type, dst_type), edges in edge_indices.items():
        if edges:
            sorted_edges = sorted(edges)
            edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()
            graph_data[src_type, edge_type, dst_type].edge_index = edge_index

    # --- 7. Store identifier mappings ---
    # Flat lists over all items (order matches node indices)
    graph_data.book_ids = all_item_ids  # back-compat alias
    graph_data.gtl_ids = all_gtl_ids
    graph_data.item_types = all_item_types  # per-node item_type label

    # Per-type slices for post-training embedding extraction
    graph_data.item_ids_by_type = item_ids_by_type
    graph_data.gtl_ids_by_type = gtl_ids_by_type

    # Metadata column registry
    all_node_types_for_metadata = [*gtl_nodes.keys(), *shared_nodes.keys()]
    graph_data.metadata_columns = all_node_types_for_metadata

    metadata_ids_by_column: dict[str, list[str]] = {
        **gtl_ids_map,
        **shared_ids,
    }
    graph_data.metadata_ids_by_column = metadata_ids_by_column

    metadata_keys: list[MetadataKey] = []
    for col in all_node_types_for_metadata:
        for value in metadata_ids_by_column.get(col, []):
            metadata_keys.append((col, value))
    graph_data.metadata_ids = metadata_keys

    return graph_data


def build_book_metadata_heterograph(
    parquet_path: str,
    *,
    nrows: int | None = None,
    filters: Sequence[tuple[str, str, Iterable[object]]] | None = None,
) -> HeteroData:
    """Load a parquet file and build the corresponding item-metadata graph.

    If the parquet file contains an ``item_type`` column, the multi-type
    builder is used automatically (supporting books, music, etc.).
    Otherwise the legacy book-only path is used.
    """
    read_kwargs: dict[str, object] = {}
    if filters is not None:
        read_kwargs["filters"] = list(filters)

    df = pd.read_parquet(parquet_path, **read_kwargs)
    if nrows is not None:
        df = df.sample(min(len(df), nrows), random_state=42)

    data_graph = build_book_metadata_heterograph_from_dataframe(
        df,
        metadata_columns=DEFAULT_METADATA_COLUMNS,
    )

    return data_graph
