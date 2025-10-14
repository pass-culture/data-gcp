from enum import Enum
from typing import Any

import networkx as nx
import torch
from loguru import logger
from torch_geometric.data import Data, HeteroData
from torch_geometric.utils import to_networkx, to_undirected


class PruningStrategy(Enum):
    """Enumeration of supported graph pruning strategies."""

    REMOVE_SMALL = "remove_small"  # Remove components smaller than threshold
    KEEP_LARGEST = "keep_largest"  # Keep only K largest components

    @classmethod
    def default(cls):
        """Return the default pruning strategy."""
        return cls.REMOVE_SMALL


def validate_pruning_params(params: dict[str, Any]) -> None:
    """Validate pruning parameters match the selected strategy."""
    strategy = params.get("strategy")

    if strategy == PruningStrategy.REMOVE_SMALL and "min_size" not in params:
        raise ValueError("REMOVE_SMALL strategy requires 'min_size'")

    if strategy == PruningStrategy.KEEP_LARGEST and "k" not in params:
        raise ValueError("KEEP_LARGEST strategy requires 'k'")


# Default configurations
DEFAULT_PRUNING = {"strategy": PruningStrategy.REMOVE_SMALL, "min_size": 2}


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
    """Return a filtered copy of the graph keeping only given global node IDs."""
    filtered = HeteroData()

    # Filter nodes
    for node_type in graph.node_types:
        offset = node_type_offsets[node_type]
        n_nodes = graph[node_type].num_nodes
        local_idx = torch.arange(offset, offset + n_nodes)
        keep_mask = torch.tensor([idx.item() in keep_nodes for idx in local_idx])
        keep_indices = torch.nonzero(keep_mask, as_tuple=True)[0]

        for key, value in graph[node_type].items():
            if torch.is_tensor(value) and value.size(0) == n_nodes:
                filtered[node_type][key] = value[keep_indices]
        filtered[node_type].num_nodes = len(keep_indices)

    # Filter edges
    for edge_type in graph.edge_types:
        src, rel, dst = edge_type
        edge_index = graph[edge_type].edge_index
        src_offset = node_type_offsets[src]
        dst_offset = node_type_offsets[dst]

        global_src = edge_index[0] + src_offset
        global_dst = edge_index[1] + dst_offset

        keep_edge_mask = [
            int(s.item()) in keep_nodes and int(d.item()) in keep_nodes
            for s, d in zip(global_src, global_dst, strict=False)
        ]
        keep_edge_mask = torch.tensor(keep_edge_mask)
        filtered_edges = edge_index[:, keep_edge_mask]

        filtered[edge_type].edge_index = filtered_edges

    return filtered


# ======================================================
#  PRUNE SMALL COMPONENTS
# ======================================================


def prune_small_components(
    graph: HeteroData, min_size: int, components_data: tuple | None = None
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


# ======================================================
#  KEEP LARGEST COMPONENTS
# ======================================================


def keep_largest_components(
    graph: HeteroData, k: int, components_data: tuple | None = None
) -> HeteroData:
    """Keep only the nodes and edges belonging to the `k` largest connected components.

    Args:
        graph (HeteroData): Input heterogeneous graph.
        k (int): Number of largest connected components to keep.
        components_data (tuple, optional): Precomputed output from `get_connected_components()`.
            If None, it will be computed internally.

    Returns:
        HeteroData: Filtered graph containing only the `k` largest components.
    """
    if components_data is None:
        components_data = get_connected_components(graph)

    components, component_sizes, total_nodes, node_type_offsets = components_data
    sorted_components = sorted(
        zip(components, component_sizes, strict=False), key=lambda x: x[1], reverse=True
    )
    kept_components = sorted_components[:k]

    keep_nodes = set().union(*[c for c, _ in kept_components])
    logger.info(
        f"Keeping {k} largest components: {len(keep_nodes):,}/{total_nodes:,} "
        f"nodes ({len(keep_nodes) / total_nodes * 100:.1f}%)"
    )

    kept_graph = _filter_graph_by_nodes(graph, keep_nodes, node_type_offsets)
    logger.info(f"✅ Retained {k} largest components successfully.")
    return kept_graph


# ======================================================
#  AGNOSTIC PRUNING
# ======================================================


def prune_graph_components(
    graph: HeteroData | Data,
    *,
    strategy: PruningStrategy,
    min_size: int | None = None,
    k: int | None = None,
    components_data: tuple | None = None,
) -> HeteroData:
    """Prune or keep connected components in a heterogeneous graph
    based on the selected pruning strategy.

    Args:
        graph (HeteroData | Data): Input graph.
        strategy (PruningStrategy): Pruning strategy to apply.
            - PruningStrategy.REMOVE_SMALL → remove all components ≤ `min_size`
            - PruningStrategy.KEEP_LARGEST → keep only `k` largest components
        min_size (int, optional): Minimum component size to keep (required for REMOVE_SMALL).
        k (int, optional): Number of largest components to keep (required for KEEP_LARGEST).
        components_data (tuple, optional): Precomputed connected component data
            from `get_connected_components()`. If None, computed internally.

    Returns:
        HeteroData: The pruned graph according to the selected strategy.

    Raises:
        ValueError: If required parameters are missing for the chosen strategy.
    """
    if strategy == PruningStrategy.REMOVE_SMALL:
        if min_size is None:
            raise ValueError(
                "`min_size` must be provided when strategy=PruningStrategy.REMOVE_SMALL"
            )
        logger.info(f"🧹 Pruning components with size ≤ {min_size} ...")
        return prune_small_components(graph, min_size, components_data)

    elif strategy == PruningStrategy.KEEP_LARGEST:
        if k is None:
            raise ValueError(
                "`k` must be provided when strategy=PruningStrategy.KEEP_LARGEST"
            )
        logger.info(f"🏗️ Keeping top {k} largest connected components ...")
        return keep_largest_components(graph, k, components_data)

    else:
        raise ValueError(f"Unknown pruning strategy: {strategy}")
