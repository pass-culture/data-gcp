import torch
from loguru import logger
from scipy.sparse.csgraph import connected_components
from torch_geometric.data import Data, HeteroData
from torch_geometric.utils import to_scipy_sparse_matrix, to_undirected

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
