
import networkx as nx
import torch
from loguru import logger
from torch_geometric.data import Data, HeteroData
from torch_geometric.utils import to_networkx, to_undirected

from src.utils.graph_indexing import (
    compute_old_to_new_index_mapping,
    remap_edge_indices,
    update_graph_identifiers_after_filtering,
)

GRAPH_PRUNING_MIN_SIZE = 2

# ======================================================
#  CONNECTED COMPONENTS UTILITIES
# ======================================================


def get_connected_components(graph: Data | HeteroData):
    """Compute connected components for a (heterogeneous) PyG graph.

    Args:
        graph (Data | HeteroData): Input graph.

    Returns:
        tuple:
            components (list[set[int]]): List of node sets, one per connected component.
            component_sizes (list[int]): Size of each component.
            total_nodes (int): Total number of nodes across all types.
            node_type_offsets (dict[str, int]): Global index offset for each node type.
    """
    # Map to global node index offsets
    node_type_offsets = {}
    current_offset = 0
    for node_type in graph.node_types:
        node_type_offsets[node_type] = current_offset
        current_offset += graph[node_type].num_nodes

    # Collect all edges into a unified graph
    all_edges = []
    for edge_type in graph.edge_types:
        src_type, rel, dst_type = edge_type
        edge_index = graph[edge_type].edge_index
        src_offset = node_type_offsets[src_type]
        dst_offset = node_type_offsets[dst_type]
        global_src = edge_index[0] + src_offset
        global_dst = edge_index[1] + dst_offset
        all_edges.append(torch.stack([global_src, global_dst], dim=0))

    combined_edges = torch.cat(all_edges, dim=1)
    combined_edges = to_undirected(combined_edges)

    total_nodes = sum(graph[nt].num_nodes for nt in graph.node_types)
    G = to_networkx(
        Data(edge_index=combined_edges, num_nodes=total_nodes), to_undirected=True
    )

    components = list(nx.connected_components(G))
    component_sizes = [len(c) for c in components]
    return components, component_sizes, total_nodes, node_type_offsets


# ======================================================
#  DIAGNOSTIC FUNCTION
# ======================================================


def diagnose_component_sizes(graph: Data | HeteroData) -> tuple:
    """Analyze connected component sizes and return them.

    Args:
        graph (Data | HeteroData): Input graph.

    Returns:
        list[set[int]]: Connected components (each a set of node indices).
    """
    components_data = (components, component_sizes, total_nodes, node_type_offsets) = (
        get_connected_components(graph)
    )

    logger.info("\n" + "=" * 60)
    logger.info("CONNECTED COMPONENTS DETAILED ANALYSIS")
    logger.info("=" * 60)
    logger.info(f"Total connected components: {len(components):,}")

    # Sort for readability (largest first)
    component_sizes_sorted = sorted(component_sizes, reverse=True)
    logger.info("\nComponent Size Distribution:")
    logger.info(
        f"  Largest: {component_sizes_sorted[0]:,} "
        f"({component_sizes_sorted[0] / total_nodes * 100:.1f}%)"
    )

    if len(component_sizes_sorted) > 1:
        logger.info(
            f"  2nd largest: {component_sizes_sorted[1]:,} "
            f"({component_sizes_sorted[1] / total_nodes * 100:.1f}%)"
        )
    if len(component_sizes_sorted) > 2:
        logger.info(
            f"  3rd largest: {component_sizes_sorted[2]:,} "
            f"({component_sizes_sorted[2] / total_nodes * 100:.1f}%)"
        )

    size_buckets = {
        "Size 2 (isolated)": sum(1 for s in component_sizes_sorted if s < 2),
        "Size 3-10": sum(1 for s in component_sizes_sorted if 3 <= s <= 10),
        "Size 11-100": sum(1 for s in component_sizes_sorted if 11 <= s <= 100),
        "Size 101-1000": sum(1 for s in component_sizes_sorted if 101 <= s <= 1000),
        "Size 1001-10000": sum(1 for s in component_sizes_sorted if 1001 <= s <= 10000),
        "Size 10001+": sum(1 for s in component_sizes_sorted if s > 10000),
    }

    logger.info("\nComponents by Size Bucket:")
    for bucket, count in size_buckets.items():
        logger.info(f"  {bucket}: {count:,}")

    isolated_nodes = sum(s for s in component_sizes_sorted if s == 1)
    logger.info(
        f"\n⚠️  Isolated nodes: {isolated_nodes:,} "
        f"({isolated_nodes / total_nodes * 100:.1f}%)"
    )
    logger.info(
        f"   Main component coverage: {component_sizes_sorted[0] / total_nodes * 100:.1f}%"
    )

    return components_data


# ======================================================
#  FILTER HELPERS (reused by pruning & keeping)
# ======================================================


def _filter_graph_by_nodes(
    graph: HeteroData, keep_nodes: set[int], node_type_offsets: dict[str, int]
) -> HeteroData:
    """Return a filtered copy of the graph keeping only given global node IDs.

    This function:
    1. Filters nodes to keep only those in keep_nodes
    2. Renumbers nodes to be contiguous (0, 1, 2, ...)
    3. Filters edges to keep only those connecting kept nodes
    4. Remaps edge indices to use the new node numbering
    5. Updates custom attributes (book_ids, metadata mappings, etc.)
    """
    filtered = HeteroData()

    # Track old->new index mappings for each node type
    old_to_new_mappings: dict[str, dict[int, int]] = {}

    # Step 1: Filter nodes and build index remapping
    for node_type in graph.node_types:
        offset = node_type_offsets[node_type]
        n_nodes = graph[node_type].num_nodes

        # Find which local indices to keep
        keep_local_indices = [i for i in range(n_nodes) if (offset + i) in keep_nodes]

        # Build old->new index mapping using utility
        old_to_new_mappings[node_type] = compute_old_to_new_index_mapping(
            n_original=n_nodes,
            keep_indices=keep_local_indices,
        )

        # Filter node features
        keep_mask = torch.tensor(keep_local_indices, dtype=torch.long)
        for key, value in graph[node_type].items():
            if torch.is_tensor(value) and value.size(0) == n_nodes:
                filtered[node_type][key] = value[keep_mask]
        filtered[node_type].num_nodes = len(keep_local_indices)

    # Step 2: Filter and remap edges
    for edge_type in graph.edge_types:
        src_type, rel, dst_type = edge_type
        edge_index = graph[edge_type].edge_index
        src_offset = node_type_offsets[src_type]
        dst_offset = node_type_offsets[dst_type]

        # Convert to global indices to check if edges should be kept
        global_src = edge_index[0] + src_offset
        global_dst = edge_index[1] + dst_offset

        # Keep only edges where both endpoints are in keep_nodes
        keep_edge_mask = torch.tensor(
            [
                int(s.item()) in keep_nodes and int(d.item()) in keep_nodes
                for s, d in zip(global_src, global_dst, strict=False)
            ]
        )

        if keep_edge_mask.any():
            # Get edges with old local indices
            kept_edges = edge_index[:, keep_edge_mask]

            # Remap to new local indices using utility
            remapped_edges = remap_edge_indices(
                edge_index=kept_edges,
                src_mapping=old_to_new_mappings[src_type],
                dst_mapping=old_to_new_mappings[dst_type],
            )

            filtered[edge_type].edge_index = remapped_edges

    # Step 3: Update custom attributes (book_ids, metadata mappings, etc.)
    update_graph_identifiers_after_filtering(
        original_graph=graph,
        filtered_graph=filtered,
        keep_nodes=keep_nodes,
        node_type_offsets=node_type_offsets,
    )

    return filtered


# ======================================================
#  PRUNE SMALL COMPONENTS
# ======================================================


def prune_small_components(
    graph: HeteroData,
    min_size: int = GRAPH_PRUNING_MIN_SIZE,
    components_data: tuple | None = None,
) -> HeteroData:
    """Remove all nodes in components of size ≤ min_size.

    Args:
        graph (HeteroData): Input heterogeneous graph.
        min_size (int): Minimum component size to keep.
        components_data (tuple, optional): Precomputed output from `get_connected_components()`.
            If None, it will be computed internally.

    Returns:
        HeteroData: Pruned graph.
    """
    if components_data is None:
        components_data = get_connected_components(graph)

    components, component_sizes, total_nodes, node_type_offsets = components_data
    keep_nodes = set().union(
        *[
            c
            for c, size in zip(components, component_sizes, strict=False)
            if size > min_size
        ]
    )

    logger.info(f"Pruning components with size ≤ {min_size} ...")
    logger.info(
        f"Keeping {len(keep_nodes):,}/{total_nodes:,} nodes "
        f"({len(keep_nodes) / total_nodes * 100:.1f}%)"
    )

    pruned_graph = _filter_graph_by_nodes(graph, keep_nodes, node_type_offsets)
    logger.info("✅ Pruning complete.")
    return pruned_graph
