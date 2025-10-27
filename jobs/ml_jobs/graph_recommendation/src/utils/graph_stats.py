"""Graph statistics utilities."""

import pandas as pd
import torch
from scipy.sparse.csgraph import connected_components
from torch_geometric.data import Data, HeteroData
from torch_geometric.utils import to_scipy_sparse_matrix, to_undirected

GraphType = Data | HeteroData


# ======================================================
# CONNECTED COMPONENTS
# ======================================================
def get_connected_components(
    graph: GraphType,
) -> tuple[list[set[int]], list[int], int, dict[str, int]]:
    """
    Compute connected components of a graph using scipy.

    Args:
        graph: Input graph (Data or HeteroData).

    Returns:
        components: list of sets of global node indices per component.
        component_sizes: Sizes of each component.
        total_nodes: Total number of nodes.
        node_type_offsets: Mapping from node type to starting global index.
    """
    # Map to global node index offsets
    node_type_offsets: dict[str, int] = {}
    offset = 0
    for nt in graph.node_types:
        node_type_offsets[nt] = offset
        offset += graph[nt].num_nodes

    # Collect all edges
    all_edges: list[torch.Tensor] = []
    for et in graph.edge_types:
        src_type, _, dst_type = et
        edge_index = graph[et].edge_index
        src_offset = node_type_offsets[src_type]
        dst_offset = node_type_offsets[dst_type]
        all_edges.append(
            torch.stack([edge_index[0] + src_offset, edge_index[1] + dst_offset], dim=0)
        )

    combined_edges = torch.cat(all_edges, dim=1)
    combined_edges = to_undirected(combined_edges)

    total_nodes = sum(graph[nt].num_nodes for nt in graph.node_types)
    adj_matrix = to_scipy_sparse_matrix(combined_edges, num_nodes=total_nodes)
    n_components, labels = connected_components(adj_matrix, directed=False)
    labels_tensor = torch.from_numpy(labels)

    component_sizes = torch.bincount(labels_tensor).tolist()
    components = [
        set(torch.where(labels_tensor == i)[0].tolist()) for i in range(n_components)
    ]

    return components, component_sizes, total_nodes, node_type_offsets


# ======================================================
# GLOBAL-LOCAL NODE MAPPING
# ======================================================
def build_global_to_local_mapping(
    graph: GraphType, node_type_offsets: dict[str, int]
) -> dict[int, tuple[str, int]]:
    """
    Map global node index to (node_type, local_index).

    Args:
        graph: Input graph.
        node_type_offsets: Node type -> global offset.

    Returns:
        Mapping from global index to (node_type, local index)
    """
    mapping: dict[int, tuple[str, int]] = {}
    for nt, offset in node_type_offsets.items():
        mapping.update({offset + i: (nt, i) for i in range(graph[nt].num_nodes)})
    return mapping


# ======================================================
# SUMMARY DATAFRAME
# ======================================================
def build_summary_df(
    graph: GraphType,
    components: list[set[int]],
    component_sizes: list[int],
    total_nodes: int,
    node_type_offsets: dict[str, int],
    global_to_local: dict[int, tuple[str, int]],
) -> pd.DataFrame:
    """
    Build a summary DataFrame with graph-level statistics.

    Args:
        graph: Input graph.
        components: list of connected components.
        component_sizes: Sizes of components.
        total_nodes: Total number of nodes.
        node_type_offsets: Node type -> global index offset.
        global_to_local: Global index -> (node_type, local index).

    Returns:
        DataFrame with graph-level measures.
    """
    largest_component_idx = (
        component_sizes.index(max(component_sizes)) if components else -1
    )
    largest_component = (
        components[largest_component_idx] if largest_component_idx >= 0 else set()
    )

    # Node type counts in largest component
    largest_nodes = torch.tensor(list(largest_component))
    node_types_tensor = torch.zeros(len(largest_nodes), dtype=torch.int)
    nt_to_idx = {nt: i for i, nt in enumerate(graph.node_types)}
    for i, idx in enumerate(largest_nodes):
        nt, _ = global_to_local[int(idx)]
        node_types_tensor[i] = nt_to_idx[nt]
    largest_comp_node_counts = {
        nt: int((node_types_tensor == i).sum()) for nt, i in nt_to_idx.items()
    }

    # Edge counts in largest component (vectorized)
    largest_comp_edge_counts: dict[str, int] = {}
    largest_comp_total_edges = 0
    for et in graph.edge_types:
        src_type, rel, dst_type = et
        edge_index = graph[et].edge_index
        src_offset = node_type_offsets[src_type]
        dst_offset = node_type_offsets[dst_type]
        global_src = edge_index[0] + src_offset
        global_dst = edge_index[1] + dst_offset
        mask = torch.isin(global_src, largest_nodes) & torch.isin(
            global_dst, largest_nodes
        )
        count = int(mask.sum())
        edge_str = f"{src_type}__{rel}__{dst_type}"
        largest_comp_edge_counts[edge_str] = count
        largest_comp_total_edges += count

    # Build summary results
    summary_results = [
        {"graph_measure": "total_nbr_of_nodes", "value": total_nodes},
        {
            "graph_measure": "total_nbr_of_edges",
            "value": sum(graph[et].edge_index.size(1) for et in graph.edge_types),
        },
        *[
            {"graph_measure": f"total_nbr_of_{nt}_nodes", "value": graph[nt].num_nodes}
            for nt in graph.node_types
        ],
        *[
            {
                "graph_measure": f"total_nbr_of_{src}__{rel}__{dst}_edges",
                "value": graph[et].edge_index.size(1),
            }
            for et in graph.edge_types
            for src, rel, dst in [et]
        ],
        {"graph_measure": "nbr_of_connected_components", "value": len(components)},
        {
            "graph_measure": "largest_connected_component_size",
            "value": len(largest_component),
        },
        *[
            {"graph_measure": f"largest_component_{nt}_nodes", "value": count}
            for nt, count in largest_comp_node_counts.items()
        ],
        {
            "graph_measure": "largest_component_nbr_of_edges",
            "value": largest_comp_total_edges,
        },
        *[
            {"graph_measure": f"largest_component_{et}_edges", "value": count}
            for et, count in largest_comp_edge_counts.items()
        ],
    ]
    return pd.DataFrame(summary_results)


# ======================================================
# COMPONENT-LEVEL DATAFRAME
# ======================================================
def build_components_df(
    graph: GraphType,
    components: list[set[int]],
    node_type_offsets: dict[str, int],
    global_to_local: dict[int, tuple[str, int]],
) -> pd.DataFrame:
    """
    Build a DataFrame with per-component statistics.

    Args:
        graph: Input graph.
        components: list of connected components.
        node_type_offsets: Node type -> global offset.
        global_to_local: Global index -> (node_type, local index).

    Returns:
        DataFrame with one row per component.
    """
    results = []

    for comp_id, nodes in enumerate(components):
        nodes_tensor = torch.tensor(list(nodes))
        comp_stats: dict[str, int] = {
            "component_id": comp_id,
            "component_size": len(nodes),
        }

        # Node type counts
        node_types_tensor = torch.zeros(len(nodes_tensor), dtype=torch.int)
        nt_to_idx = {nt: i for i, nt in enumerate(graph.node_types)}
        for i, idx in enumerate(nodes_tensor):
            nt, _ = global_to_local[int(idx)]
            node_types_tensor[i] = nt_to_idx[nt]
        for nt, i in nt_to_idx.items():
            comp_stats[f"{nt}_count"] = int((node_types_tensor == i).sum())

        # Edge counts (vectorized)
        total_edges = 0
        for et in graph.edge_types:
            src_type, rel, dst_type = et
            edge_index = graph[et].edge_index
            src_offset = node_type_offsets[src_type]
            dst_offset = node_type_offsets[dst_type]
            global_src = edge_index[0] + src_offset
            global_dst = edge_index[1] + dst_offset
            mask = torch.isin(global_src, nodes_tensor) & torch.isin(
                global_dst, nodes_tensor
            )
            count = int(mask.sum())
            edge_str = f"{src_type}__{rel}__{dst_type}"
            comp_stats[f"{edge_str}_count"] = count
            total_edges += count

        comp_stats["num_edges"] = total_edges
        results.append(comp_stats)

    return (
        pd.DataFrame(results)
        .sort_values("component_size", ascending=False)
        .reset_index(drop=True)
    )


# ======================================================
# MAIN GRAPH ANALYSIS
# ======================================================
def get_graph_analysis(graph: GraphType) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate graph-level and per-component statistics.

    Args:
        graph: Input graph.

    Returns:
        summary_df: DataFrame with graph-level measures.
        components_df: DataFrame with per-component statistics.
    """
    components, component_sizes, total_nodes, node_type_offsets = (
        get_connected_components(graph)
    )
    global_to_local = build_global_to_local_mapping(graph, node_type_offsets)
    summary_df = build_summary_df(
        graph,
        components,
        component_sizes,
        total_nodes,
        node_type_offsets,
        global_to_local,
    )
    components_df = build_components_df(
        graph, components, node_type_offsets, global_to_local
    )
    return summary_df, components_df
