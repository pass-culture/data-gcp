import torch
from loguru import logger
from scipy.sparse.csgraph import connected_components
from torch_geometric.data import Data, HeteroData
from torch_geometric.utils import to_scipy_sparse_matrix, to_undirected

from src.utils.graph_indexing import update_graph_identifiers_after_filtering

GRAPH_PRUNING_MIN_SIZE = 2

# ======================================================
#  GRAPH STATISTICS UTILITIES
# ======================================================


def get_graph_statistics(graph: HeteroData) -> dict:
    """Extract key statistics from a heterogeneous graph.

    Args:
        graph: The heterogeneous graph to analyze.

    Returns:
        Dictionary with counts for total nodes, nodes by type, and edges.
    """
    stats = {}

    # Count total nodes
    stats["total_nodes"] = sum(graph[nt].num_nodes for nt in graph.node_types)

    # Count nodes by type
    stats["nodes_by_type"] = {
        node_type: graph[node_type].num_nodes for node_type in graph.node_types
    }

    # Count total edges
    stats["total_edges"] = sum(graph[et].edge_index.size(1) for et in graph.edge_types)

    return stats


def log_pruning_summary(before: dict, after: dict, min_size: int) -> None:
    """Log a detailed summary of pruning results.

    Works with any heterogeneous graph structure without hardcoded node type names.

    Args:
        before: Statistics before pruning.
        after: Statistics after pruning.
        min_size: The minimum component size threshold used.
    """
    logger.info("=" * 60)
    logger.info(f"PRUNING SUMMARY (min_size={min_size})")
    logger.info("=" * 60)

    # Total nodes summary
    node_reduction = before["total_nodes"] - after["total_nodes"]
    node_pct = (
        (node_reduction / before["total_nodes"] * 100)
        if before["total_nodes"] > 0
        else 0
    )
    logger.info(
        f"Total Nodes: {before['total_nodes']:,} → {after['total_nodes']:,} "
        f"(-{node_reduction:,}, -{node_pct:.1f}%)"
    )

    # Nodes by type breakdown
    if before["nodes_by_type"]:
        logger.info("\n  Nodes by Type:")
        # Get all node types (union of before and after)
        all_node_types = sorted(
            set(before["nodes_by_type"].keys()) | set(after["nodes_by_type"].keys())
        )

        for node_type in all_node_types:
            before_count = before["nodes_by_type"].get(node_type, 0)
            after_count = after["nodes_by_type"].get(node_type, 0)
            reduction = before_count - after_count
            pct = (reduction / before_count * 100) if before_count > 0 else 0

            logger.info(
                f"    {node_type}: {before_count:,} → {after_count:,} "
                f"(-{reduction:,}, -{pct:.1f}%)"
            )

    # Total edges summary
    edge_reduction = before["total_edges"] - after["total_edges"]
    edge_pct = (
        (edge_reduction / before["total_edges"] * 100)
        if before["total_edges"] > 0
        else 0
    )
    logger.info(
        f"\nTotal Edges: {before['total_edges']:,} → {after['total_edges']:,} "
        f"(-{edge_reduction:,}, -{edge_pct:.1f}%)"
    )

    logger.info("=" * 60)


# ======================================================
#  CONNECTED COMPONENTS UTILITIES
# ======================================================


def get_connected_components(graph: Data | HeteroData):
    """Compute connected components using scipy (more efficient).

    Args:
        graph: Input graph (Data or HeteroData).

    Returns:
        tuple: (components, component_sizes, total_nodes, node_type_offsets)
            - components: List of sets, each containing node indices in a component
            - component_sizes: List of component sizes
            - total_nodes: Total number of nodes
            - node_type_offsets: Dict mapping node types to global index offsets
    """
    # Map to global node index offsets
    node_type_offsets = {}
    current_offset = 0
    for node_type in graph.node_types:
        node_type_offsets[node_type] = current_offset
        current_offset += graph[node_type].num_nodes

    # Collect all edges
    all_edges = []
    for edge_type in graph.edge_types:
        src_type, _rel, dst_type = edge_type
        edge_index = graph[edge_type].edge_index
        src_offset = node_type_offsets[src_type]
        dst_offset = node_type_offsets[dst_type]
        global_src = edge_index[0] + src_offset
        global_dst = edge_index[1] + dst_offset
        all_edges.append(torch.stack([global_src, global_dst], dim=0))

    combined_edges = torch.cat(all_edges, dim=1)
    combined_edges = to_undirected(combined_edges)

    total_nodes = sum(graph[nt].num_nodes for nt in graph.node_types)

    # Use scipy for connected components
    adj_matrix = to_scipy_sparse_matrix(combined_edges, num_nodes=total_nodes)
    n_components, labels = connected_components(adj_matrix, directed=False)

    # Vectorized component grouping
    labels_tensor = torch.from_numpy(labels)

    # Get component sizes using bincount
    component_sizes = torch.bincount(labels_tensor).tolist()

    # Group nodes by component (vectorized)
    components = []
    for comp_idx in range(n_components):
        node_indices = torch.where(labels_tensor == comp_idx)[0]
        components.append(set(node_indices.tolist()))

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
    components_data = (components, component_sizes, total_nodes, _node_type_offsets) = (
        get_connected_components(graph)
    )

    logger.info("=" * 60)
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
        f"Main component coverage: {component_sizes_sorted[0] / total_nodes * 100:.1f}%"
    )

    return components_data


# ======================================================
#  FILTER HELPERS (reused by pruning & keeping)
# ======================================================


def _filter_graph_by_nodes(
    graph: HeteroData, keep_nodes: set[int], node_type_offsets: dict[str, int]
) -> HeteroData:
    """Filter graph using PyG's built-in subgraph method.

    PyG's subgraph() automatically handles:
    - Node filtering and reindexing (makes indices contiguous)
    - Edge filtering (removes edges with deleted endpoints)
    - Edge reindexing (updates edge_index to new node numbers)
    - Feature filtering (node and edge attributes)

    We only need to handle custom attributes (book_ids, metadata mappings).

    Args:
        graph: Input heterogeneous graph
        keep_nodes: Set of global node indices to keep
        node_type_offsets: Mapping of node types to their global index offsets

    Returns:
        Filtered graph with updated attributes
    """
    # Convert global keep_nodes to per-type local indices for PyG
    subset_dict = {}
    for node_type in graph.node_types:
        offset = node_type_offsets[node_type]
        n_nodes = graph[node_type].num_nodes

        # Find which local indices to keep for this node type
        local_indices = [i for i in range(n_nodes) if (offset + i) in keep_nodes]
        subset_dict[node_type] = torch.tensor(local_indices, dtype=torch.long)

    # PyG does ALL the filtering and reindexing!
    filtered = graph.subgraph(subset_dict)

    # Update our custom attributes (book_ids, metadata_ids_by_column, etc.)
    # PyG doesn't know about these, so we handle them ourselves
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
        graph: Input heterogeneous graph.
        min_size: Minimum component size to keep.
        components_data: Precomputed output from `get_connected_components()`.
            If None, it will be computed internally.

    Returns:
        Pruned graph.
    """
    # Capture statistics before pruning
    stats_before = get_graph_statistics(graph)

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

    if len(keep_nodes) == total_nodes:
        logger.info("Early exit, pruning not needed")
        return graph

    pruned_graph = _filter_graph_by_nodes(graph, keep_nodes, node_type_offsets)

    # Capture statistics after pruning
    stats_after = get_graph_statistics(pruned_graph)

    # Log detailed summary
    log_pruning_summary(stats_before, stats_after, min_size)

    return pruned_graph
