"""Utilities for metadata and index mapping and operations in heterogeneous graphs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import torch

if TYPE_CHECKING:
    from src.constants import MetadataKey

from torch_geometric.data import HeteroData


def build_id_to_index_map(ids: list[str]) -> dict[str, int]:
    """Build a mapping from identifiers to contiguous indices.

    Args:
        ids: List of identifiers (e.g., book IDs, metadata values).

    Returns:
        Dictionary mapping each identifier to its index position.

    Example:
        >>> build_id_to_index_map(['book_1', 'book_2', 'book_3'])
        {'book_1': 0, 'book_2': 1, 'book_3': 2}
    """
    return {id_: idx for idx, id_ in enumerate(ids)}


def compute_old_to_new_index_mapping(
    n_original: int,
    keep_indices: list[int],
) -> dict[int, int]:
    """Build mapping from old indices to new contiguous indices.

    After filtering, nodes are renumbered to be contiguous (0, 1, 2, ...).
    This function creates a mapping from old indices to their new positions.

    Args:
        n_original: Total number of items before filtering.
        keep_indices: List of old indices that are being kept (must be sorted or will be sorted).

    Returns:
        Dictionary mapping old_index -> new_index for kept indices only.

    Example:
        >>> compute_old_to_new_index_mapping(5, [1, 3, 4])
        {1: 0, 3: 1, 4: 2}
    """
    return {old_idx: new_idx for new_idx, old_idx in enumerate(keep_indices)}


def remap_edge_indices(
    edge_index: torch.Tensor,
    src_mapping: dict[int, int],
    dst_mapping: dict[int, int],
) -> torch.Tensor:
    """Remap edge indices using old->new index mappings.

    Args:
        edge_index: Tensor of shape [2, num_edges] with [source_indices, dest_indices].
        src_mapping: Dictionary mapping old source indices to new source indices.
        dst_mapping: Dictionary mapping old dest indices to new dest indices.

    Returns:
        Remapped edge_index tensor with new indices.

    Example:
        >>> edge_index = torch.tensor([[0, 2], [1, 3]])  # 2 edges
        >>> src_map = {0: 0, 2: 1}  # node 0->0, node 2->1
        >>> dst_map = {1: 0, 3: 1}  # node 1->0, node 3->1
        >>> remap_edge_indices(edge_index, src_map, dst_map)
        tensor([[0, 1], [0, 1]])
    """
    old_src = edge_index[0]
    old_dst = edge_index[1]

    new_src = torch.tensor(
        [src_mapping[idx.item()] for idx in old_src],
        dtype=torch.long,
    )
    new_dst = torch.tensor(
        [dst_mapping[idx.item()] for idx in old_dst],
        dtype=torch.long,
    )

    return torch.stack([new_src, new_dst], dim=0)


def set_graph_identifiers(
    graph: HeteroData,
    book_ids: list[str],
    metadata_ids_by_column: dict[str, list[str]],
    metadata_columns: list[str],
    book_index: dict[str, int] | None = None,
    metadata_nodes_by_column: dict[str, dict[str, int]] | None = None,
) -> None:
    """Set identifier attributes on a heterogeneous graph.

    This function adds custom attributes to track book and metadata identifiers,
    which are needed for mapping graph nodes back to their original data.

    Args:
        graph: The graph to add attributes to.
        book_ids: List of book IDs in order corresponding to book nodes.
        metadata_ids_by_column: Dictionary mapping column names to lists of metadata values.
        metadata_columns: List of metadata column names that have nodes in the graph.
        book_index: Optional dict mapping book_id -> node_index. If None, will be computed.
        metadata_nodes_by_column: Optional dict mapping column -> {value: node_index}. If None, will be computed.
    """
    graph.book_ids = list(book_ids)
    graph.metadata_ids_by_column = dict(metadata_ids_by_column)
    graph.metadata_columns = list(metadata_columns)

    # Store or compute index mappings using utility
    if book_index is not None:
        graph.book_index = dict(book_index)
    else:
        graph.book_index = build_id_to_index_map(book_ids)

    if metadata_nodes_by_column is not None:
        graph.metadata_nodes_by_column = {
            col: dict(mapping) for col, mapping in metadata_nodes_by_column.items()
        }
    else:
        graph.metadata_nodes_by_column = {
            column: build_id_to_index_map(values)
            for column, values in metadata_ids_by_column.items()
        }

    # Create flattened metadata_ids for backward compatibility
    metadata_keys: list[MetadataKey] = []
    for column in metadata_columns:
        for value in metadata_ids_by_column[column]:
            metadata_keys.append((column, value))
    graph.metadata_ids = metadata_keys


def update_graph_identifiers_after_filtering(
    original_graph: HeteroData,
    filtered_graph: HeteroData,
    keep_nodes: set[int],
    node_type_offsets: dict[str, int],
) -> None:
    """Update identifier attributes on a filtered graph.

    After filtering/pruning a graph, this function updates the custom identifier
    attributes to only include the nodes that remain in the filtered graph.
    This includes updating both the ID lists and the index mappings.

    Args:
        original_graph: The original graph with identifier attributes.
        filtered_graph: The filtered graph to update with new attributes.
        keep_nodes: Set of global node indices that were kept during filtering.
        node_type_offsets: Mapping of node types to their global index offsets.
    """
    # Only proceed if the original graph has custom attributes
    if not hasattr(original_graph, "book_ids"):
        return

    # Filter book_ids based on which book nodes were kept
    book_offset = node_type_offsets.get("book", 0)
    n_books = len(original_graph.book_ids)
    kept_book_indices = [i for i in range(n_books) if (book_offset + i) in keep_nodes]
    filtered_book_ids = [original_graph.book_ids[i] for i in kept_book_indices]

    # Rebuild book_index with new indices using utility
    filtered_book_index = build_id_to_index_map(filtered_book_ids)

    # Filter metadata_ids_by_column for each metadata type
    filtered_metadata_ids_by_column: dict[str, list[str]] = {}
    filtered_metadata_nodes_by_column: dict[str, dict[str, int]] = {}

    for column, metadata_list in original_graph.metadata_ids_by_column.items():
        if column not in node_type_offsets:
            continue

        metadata_offset = node_type_offsets[column]
        n_metadata = len(metadata_list)
        kept_metadata_indices = [
            i for i in range(n_metadata) if (metadata_offset + i) in keep_nodes
        ]

        # Only include columns that have remaining values
        if kept_metadata_indices:
            filtered_values = [metadata_list[i] for i in kept_metadata_indices]
            filtered_metadata_ids_by_column[column] = filtered_values

            # Rebuild value -> index mapping with new indices using utility
            filtered_metadata_nodes_by_column[column] = build_id_to_index_map(
                filtered_values
            )

    # Update metadata_columns to only include columns with remaining values
    filtered_metadata_columns = list(filtered_metadata_ids_by_column.keys())

    # Set the filtered attributes on the new graph
    set_graph_identifiers(
        graph=filtered_graph,
        book_ids=filtered_book_ids,
        metadata_ids_by_column=filtered_metadata_ids_by_column,
        metadata_columns=filtered_metadata_columns,
        book_index=filtered_book_index,
        metadata_nodes_by_column=filtered_metadata_nodes_by_column,
    )
