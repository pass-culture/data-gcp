"""Utilities to construct a PyTorch Geometric heterogeneous graph for item recommendations (books, music, …)."""

from __future__ import annotations

import warnings
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
# Private helpers
# ---------------------------------------------------------------------------


def _normalize_dataframe(
    dataframe: pd.DataFrame,
    all_metadata_columns: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    """Normalise the dataframe and return it with the list of known item types present."""
    df = preprocess_metadata_dataframe(
        dataframe,
        metadata_columns=[GTL_ID_COLUMN, *all_metadata_columns],
    )
    df[ITEM_TYPE_COLUMN] = df[ITEM_TYPE_COLUMN].astype(str).str.strip().str.lower()

    item_types_present = [t for t in KNOWN_ITEM_TYPES if t in df[ITEM_TYPE_COLUMN].unique()]
    unknown_types = set(df[ITEM_TYPE_COLUMN].unique()) - set(KNOWN_ITEM_TYPES)
    if unknown_types:
        warnings.warn(
            f"Unknown item_type values will be ignored: {unknown_types}",
            stacklevel=2,
        )
    return df, item_types_present


def _index_items(
    df_normalized: pd.DataFrame,
    item_types_present: list[str],
) -> tuple[list[str], list[str], list[str], dict[str, int], dict[str, list[str]], dict[str, list[str]]]:
    """Build the item index and per-type mappings."""
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

    return all_item_ids, all_gtl_ids, all_item_types, item_index, item_ids_by_type, gtl_ids_by_type


def _build_metadata_nodes(
    df_normalized: pd.DataFrame,
    columns: list[str],
) -> tuple[dict[str, dict[str, int]], dict[str, list[str]]]:
    """Build a node index and id list for a set of metadata columns."""
    nodes: dict[str, dict[str, int]] = {}
    ids: dict[str, list[str]] = {}
    for col in columns:
        if col not in df_normalized.columns:
            continue
        unique_vals = [
            str(v) for v in df_normalized[col].dropna().unique() if v is not None and str(v).strip() not in ("None", "")
        ]
        if unique_vals:
            nodes[col] = {v: i for i, v in enumerate(unique_vals)}
            ids[col] = unique_vals
    return nodes, ids


def _init_edge_indices(
    shared_nodes: dict[str, dict[str, int]],
    gtl_nodes: dict[str, dict[str, int]],
) -> dict[tuple[str, str, str], set[tuple[int, int]]]:
    """Initialise empty edge-index sets for all relation types."""
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]] = {}
    for col in list(shared_nodes) + list(gtl_nodes):
        edge_indices[("item", f"is_{col}", col)] = set()
        edge_indices[(col, f"{col}_of", "item")] = set()
    return edge_indices


def _add_item_metadata_edge(
    col: str,
    value: object,
    item_idx: int,
    node_mapping: dict[str, int],
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]],
) -> None:
    """Add a single item↔metadata edge pair to *edge_indices* (in-place)."""
    if value is None or str(value).strip() in ("None", ""):
        return
    value_str = str(value)
    if value_str in node_mapping:
        meta_idx = node_mapping[value_str]
        edge_indices[("item", f"is_{col}", col)].add((item_idx, meta_idx))
        edge_indices[(col, f"{col}_of", "item")].add((meta_idx, item_idx))


def _add_edges_for_record(
    record_dict: dict,
    item_index: dict[str, int],
    item_types_present: list[str],
    shared_nodes: dict[str, dict[str, int]],
    gtl_nodes: dict[str, dict[str, int]],
    gtl_metadata_columns: list[str],
    shared_metadata_columns: list[str],
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]],
) -> None:
    """Add edges to *edge_indices* for a single dataframe record (in-place)."""
    item_id = record_dict[ID_COLUMN]
    item_type = record_dict[ITEM_TYPE_COLUMN]

    if item_type not in item_types_present or item_id not in item_index:
        return

    item_idx = item_index[item_id]

    for col in shared_metadata_columns:
        if col in shared_nodes:
            _add_item_metadata_edge(col, record_dict.get(col), item_idx, shared_nodes[col], edge_indices)

    applicable_gtl_columns = GTL_METADATA_COLUMNS_BY_ITEM_TYPE.get(item_type, gtl_metadata_columns)
    for col in applicable_gtl_columns:
        if col in gtl_nodes:
            _add_item_metadata_edge(col, record_dict.get(col), item_idx, gtl_nodes[col], edge_indices)


def _assemble_heterodata(
    all_item_ids: list[str],
    all_gtl_ids: list[str],
    all_item_types: list[str],
    item_ids_by_type: dict[str, list[str]],
    gtl_ids_by_type: dict[str, list[str]],
    shared_nodes: dict[str, dict[str, int]],
    gtl_nodes: dict[str, dict[str, int]],
    shared_ids: dict[str, list[str]],
    gtl_ids_map: dict[str, list[str]],
    edge_indices: dict[tuple[str, str, str], set[tuple[int, int]]],
) -> HeteroData:
    """Assemble and return the final HeteroData object."""
    graph_data = HeteroData()
    graph_data["item"].num_nodes = len(all_item_ids)

    for col, node_mapping in {**shared_nodes, **gtl_nodes}.items():
        graph_data[col].num_nodes = len(node_mapping)

    for (src_type, edge_type, dst_type), edges in edge_indices.items():
        if edges:
            sorted_edges = sorted(edges)
            edge_index = torch.tensor(sorted_edges, dtype=torch.long).t().contiguous()
            graph_data[src_type, edge_type, dst_type].edge_index = edge_index

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

    # 1. Normalise
    df_normalized, item_types_present = _normalize_dataframe(dataframe, all_metadata_columns)

    # 2. Index items
    all_item_ids, all_gtl_ids, all_item_types, item_index, item_ids_by_type, gtl_ids_by_type = _index_items(
        df_normalized, item_types_present
    )

    # 3. Build metadata node indices
    shared_nodes, shared_ids = _build_metadata_nodes(df_normalized, shared_metadata_columns)
    gtl_nodes, gtl_ids_map = _build_metadata_nodes(df_normalized, gtl_metadata_columns)

    # 4. Build edges
    edge_indices = _init_edge_indices(shared_nodes, gtl_nodes)
    all_edge_columns = [
        ID_COLUMN,
        ITEM_TYPE_COLUMN,
        *[c for c in [*shared_metadata_columns, *gtl_metadata_columns] if c in df_normalized.columns],
    ]
    for record in df_normalized[all_edge_columns].itertuples(index=False):
        _add_edges_for_record(
            record._asdict(),
            item_index,
            item_types_present,
            shared_nodes,
            gtl_nodes,
            gtl_metadata_columns,
            shared_metadata_columns,
            edge_indices,
        )

    # 5. Sanity check
    if sum(len(e) for e in edge_indices.values()) == 0:
        raise ValueError("No edges were created; check that metadata columns contain values.")

    # 6. Assemble
    return _assemble_heterodata(
        all_item_ids,
        all_gtl_ids,
        all_item_types,
        item_ids_by_type,
        gtl_ids_by_type,
        shared_nodes,
        gtl_nodes,
        shared_ids,
        gtl_ids_map,
        edge_indices,
    )


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

    for _col in [*GTL_METADATA_COLUMNS, *SHARED_METADATA_COLUMNS]:
        if _col not in df.columns:
            df[_col] = None

    return build_multitype_metadata_heterograph_from_dataframe(
        df,
        gtl_metadata_columns=list(GTL_METADATA_COLUMNS),
        shared_metadata_columns=list(SHARED_METADATA_COLUMNS),
    )
