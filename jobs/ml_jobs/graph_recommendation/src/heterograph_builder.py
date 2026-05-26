"""Utilities to construct a PyTorch Geometric heterogeneous graph for item recommendations (books, music, …)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.constants import (
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
    node type.

    **GTL id prefixing** — ``gtl_id`` is prefixed at the DBT source level
    (``b-`` for books, ``m-`` for music) so that the same numeric code for
    books and music maps to distinct nodes.  ``gtl_label_level_*`` columns
    contain textual labels that are already distinct across item types by
    nature (books and music use different taxonomies) and are not prefixed.

    **Node types**
    - ``"item"`` — all items regardless of type.
    - ``gtl_label_level_1``, ``gtl_label_level_2``, … — GTL label nodes,
      one node type per level, values pooled across item types.
    - One node type per shared metadata column (``artist_id``, ``series_id``,
      ``music_label``): columns that are null for a given item type simply
      produce no edges for that type — no special handling needed.

    **Edge types** (bipartite, both directions)
    - ``("item", "is_{C}", C)``  — item → metadata node
    - ``(C, "{C}_of", "item")``  — metadata node → item

    Args:
        dataframe: Input DataFrame. Must contain ``item_type``, ``item_id``,
            ``gtl_id`` (already prefixed) and all metadata columns.
        gtl_metadata_columns: GTL label columns (``gtl_label_level_*``).
        shared_metadata_columns: All non-GTL metadata columns. Null values
            for a given item type are silently ignored.

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
    df_normalized[ITEM_TYPE_COLUMN] = df_normalized[ITEM_TYPE_COLUMN].astype(str).str.strip().str.lower()

    item_types_present = [t for t in KNOWN_ITEM_TYPES if t in df_normalized[ITEM_TYPE_COLUMN].unique()]
    unknown_types = set(df_normalized[ITEM_TYPE_COLUMN].unique()) - set(KNOWN_ITEM_TYPES)
    if unknown_types:
        import warnings

        warnings.warn(
            f"Unknown item_type values will be ignored: {unknown_types}",
            stacklevel=2,
        )

    # --- 2. Index ALL items under a single "item" node type ---
    df_items = (
        df_normalized[[ID_COLUMN, GTL_ID_COLUMN, ITEM_TYPE_COLUMN]]
        .drop_duplicates(subset=[ID_COLUMN])
        .sort_values([ITEM_TYPE_COLUMN, ID_COLUMN])
        .reset_index(drop=True)
    )
    df_items = df_items[df_items[ITEM_TYPE_COLUMN].isin(item_types_present)]

    all_item_ids: list[str] = df_items[ID_COLUMN].tolist()
    all_gtl_ids: list[str] = df_items[GTL_ID_COLUMN].tolist()
    all_item_types: list[str] = df_items[ITEM_TYPE_COLUMN].tolist()
    item_index: dict[str, int] = {iid: idx for idx, iid in enumerate(all_item_ids)}

    item_ids_by_type: dict[str, list[str]] = {}
    gtl_ids_by_type: dict[str, list[str]] = {}
    for item_type in item_types_present:
        mask = df_items[ITEM_TYPE_COLUMN] == item_type
        item_ids_by_type[item_type] = df_items.loc[mask, ID_COLUMN].tolist()
        gtl_ids_by_type[item_type] = df_items.loc[mask, GTL_ID_COLUMN].tolist()

    # --- 3. Build metadata node indices ---
    # 3a. Shared metadata (pooled across all item types)
    shared_nodes: dict[str, dict[str, int]] = {}
    shared_ids: dict[str, list[str]] = {}
    for col in shared_metadata_columns:
        if col not in df_normalized.columns:
            continue
        unique_vals = [
            str(v) for v in df_normalized[col].dropna().unique() if v is not None and str(v).strip() not in ("None", "")
        ]
        if unique_vals:
            shared_nodes[col] = {v: i for i, v in enumerate(unique_vals)}
            shared_ids[col] = unique_vals

    # 3b. GTL metadata — single node type per column, values pooled across item types.
    gtl_nodes: dict[str, dict[str, int]] = {}
    gtl_ids_map: dict[str, list[str]] = {}
    for col in gtl_metadata_columns:
        if col not in df_normalized.columns:
            continue
        unique_vals = [
            str(v) for v in df_normalized[col].dropna().unique() if v is not None and str(v).strip() not in ("None", "")
        ]
        if unique_vals:
            gtl_nodes[col] = {v: i for i, v in enumerate(unique_vals)}
            gtl_ids_map[col] = unique_vals

    # --- 4. Build edge index sets ---
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]] = {}

    def _ensure_edge(key: tuple[str, str, str]) -> None:
        if key not in edge_indices:
            edge_indices[key] = set()

    # Same edge types for all item types (metapaths are symmetric)
    for col in shared_metadata_columns:
        if col in shared_nodes:
            _ensure_edge(("item", f"is_{col}", col))
            _ensure_edge((col, f"{col}_of", "item"))
    for col in gtl_metadata_columns:
        if col in gtl_nodes:
            _ensure_edge(("item", f"is_{col}", col))
            _ensure_edge((col, f"{col}_of", "item"))

    all_edge_columns = [
        ID_COLUMN,
        ITEM_TYPE_COLUMN,
        *[c for c in [*shared_metadata_columns, *gtl_metadata_columns] if c in df_normalized.columns],
    ]

    for record in df_normalized[all_edge_columns].itertuples(index=False):
        record_dict = record._asdict()
        item_id = record_dict[ID_COLUMN]
        item_type = record_dict[ITEM_TYPE_COLUMN]

        if item_type not in item_types_present:
            continue
        if item_id not in item_index:
            continue

        item_idx = item_index[item_id]

        # Shared metadata edges
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

        # GTL edges (values already prefixed — same edge type for all item types)
        applicable_gtl_columns = GTL_METADATA_COLUMNS_BY_ITEM_TYPE.get(item_type, gtl_metadata_columns)
        for col in applicable_gtl_columns:
            if col not in gtl_nodes:
                continue
            value = record_dict.get(col)
            if value is None or str(value).strip() in ("None", ""):
                continue
            value_str = str(value)
            if value_str in gtl_nodes[col]:
                meta_idx = gtl_nodes[col][value_str]
                edge_indices[("item", f"is_{col}", col)].add((item_idx, meta_idx))
                edge_indices[(col, f"{col}_of", "item")].add((meta_idx, item_idx))

    # --- 5. Sanity check ---
    total_edges = sum(len(e) for e in edge_indices.values())
    if total_edges == 0:
        raise ValueError("No edges were created; check that metadata columns contain values.")

    # --- 6. Assemble HeteroData ---
    graph_data = HeteroData()

    graph_data["item"].num_nodes = len(all_item_ids)

    for col, node_mapping in shared_nodes.items():
        graph_data[col].num_nodes = len(node_mapping)

    for col, node_mapping in gtl_nodes.items():
        graph_data[col].num_nodes = len(node_mapping)

    for (src_type, edge_type, dst_type), edges in edge_indices.items():
        if edges:
            sorted_edges = sorted(edges)
            edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()
            graph_data[src_type, edge_type, dst_type].edge_index = edge_index

    # --- 7. Store identifier mappings ---
    graph_data.book_ids = all_item_ids
    graph_data.gtl_ids = all_gtl_ids
    graph_data.item_types = all_item_types
    graph_data.item_ids_by_type = item_ids_by_type
    graph_data.gtl_ids_by_type = gtl_ids_by_type

    all_metadata_node_types = [*gtl_nodes.keys(), *shared_nodes.keys()]
    graph_data.metadata_columns = all_metadata_node_types

    metadata_ids_by_column: dict[str, list[str]] = {**gtl_ids_map, **shared_ids}
    graph_data.metadata_ids_by_column = metadata_ids_by_column

    metadata_keys: list[MetadataKey] = []
    for col in all_metadata_node_types:
        for value in metadata_ids_by_column.get(col, []):
            metadata_keys.append((col, value))
    graph_data.metadata_ids = metadata_keys

    return graph_data


def build_heterograph_from_parquet(
    parquet_path: str,
    *,
    nrows: int | None = None,
    filters: Sequence[tuple[str, str, Iterable[object]]] | None = None,
) -> HeteroData:
    """Load a parquet file and build the corresponding item-metadata heterograph.

    Supports multiple item types (books, music, …) — the parquet file must
    contain an ``item_type`` column. GTL metadata columns and shared metadata
    columns are taken from :mod:`src.constants`.
    """
    read_kwargs: dict[str, object] = {}
    if filters is not None:
        read_kwargs["filters"] = list(filters)

    df = pd.read_parquet(parquet_path, **read_kwargs)
    if nrows is not None:
        df = df.sample(min(len(df), nrows), random_state=42)

    # Ensure all expected metadata columns exist (missing ones produce no edges)
    for _col in [*GTL_METADATA_COLUMNS, *SHARED_METADATA_COLUMNS]:
        if _col not in df.columns:
            df[_col] = None

    data_graph = build_multitype_metadata_heterograph_from_dataframe(
        df,
        gtl_metadata_columns=list(GTL_METADATA_COLUMNS),
        shared_metadata_columns=list(SHARED_METADATA_COLUMNS),
    )

    return data_graph
