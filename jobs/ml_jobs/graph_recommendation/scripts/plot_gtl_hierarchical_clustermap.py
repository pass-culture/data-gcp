"""
Hierarchical clustermap of GTL x item_type cluster centroids.

For each (gtl_id, item_type) group, a centroid embedding is computed.
The pairwise cosine distances between centroids are then displayed as a
seaborn clustermap: rows and columns are reordered by average-linkage
hierarchical clustering, and dendrograms are shown on both axes.

Visual encoding:
  - Color scale : green (close) → red (far)
  - Row / column color strip : blue = book, orange = music

Usage:
    uv run python scripts/plot_gtl_hierarchical_clustermap.py \
        results/embeddings.parquet \\
        --raw-data   data/raw_input/book_music_catalog \\
        --output     results/clustermap.png
"""

from __future__ import annotations

import argparse

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.cluster.hierarchy import linkage as scipy_linkage
from scipy.spatial.distance import squareform
from sklearn.metrics.pairwise import cosine_distances


def load_embeddings(parquet_path: str, raw_data_path: str) -> pd.DataFrame:
    """Load embeddings and join item_type and GTL code from the raw dataset.

    Only GTL level-2 codes (ending in '0000', excluding '00000000') are kept.

    Args:
        parquet_path:   Path to the embeddings parquet file.
        raw_data_path:  Path to the raw parquet file or directory.

    Returns:
        Filtered DataFrame with columns:
        node_ids, embedding, item_id, item_type, gtl_id.
    """
    raw = pd.read_parquet(raw_data_path).loc[:, ["item_id", "item_type", "raw_gtl_id"]]
    df = (
        pd.read_parquet(parquet_path)
        .drop(columns=["gtl_id"], errors="ignore")
        .merge(raw, left_on="node_ids", right_on="item_id", how="left")
        .rename(columns={"raw_gtl_id": "gtl_id"})
    )

    print(
        f"Loaded {len(df)} embeddings"
        f" | item_types: {df['item_type'].value_counts().to_dict()}"
    )

    df = df[
        df["gtl_id"].notna()
        & df["gtl_id"].astype(str).str.endswith("0000")
        & (df["gtl_id"].astype(str) != "00000000")
    ].copy()

    return df


def compute_centroids(df: pd.DataFrame) -> pd.DataFrame:
    """Compute one centroid embedding per (gtl_id, item_type) group.

    Args:
        df: Filtered DataFrame from load_embeddings().

    Returns:
        DataFrame with columns: cluster, gtl_id, item_type, n_items, centroid.
        Sorted by (item_type, gtl_id) and index reset.
    """
    embeddings = np.stack(df["embedding"].values)

    records = []
    for (gtl_id, item_type), group in df.groupby(["gtl_id", "item_type"]):
        positions = [df.index.get_loc(i) for i in group.index]
        centroid = embeddings[positions].mean(axis=0)
        records.append(
            {
                "cluster": f"{gtl_id}__{item_type}",
                "gtl_id": gtl_id,
                "item_type": item_type,
                "n_items": len(group),
                "centroid": centroid,
            }
        )

    return (
        pd.DataFrame(records)
        .sort_values(["item_type", "gtl_id"])
        .reset_index(drop=True)
    )


def plot_clustermap(centroids_df: pd.DataFrame, output_path: str | None) -> None:
    """Plot a hierarchical clustermap of pairwise cosine distances between centroids.

    Row and column ordering follows average-linkage hierarchical clustering.
    Row/column color strips indicate item type (blue = book, orange = music).

    Args:
        centroids_df: Output of compute_centroids().
        output_path:  Save path (PNG). Displays interactively when None.
    """
    centroid_matrix = np.stack(centroids_df["centroid"].values)
    dist_matrix = cosine_distances(centroid_matrix)
    labels = centroids_df["cluster"].tolist()
    n = len(labels)

    short_labels = [
        f"{c.split('__')[0]}\n({'b' if 'book' in c else 'm'})" for c in labels
    ]
    row_colors = pd.Series(
        ["#1f77b4" if "book" in c else "#ff7f0e" for c in labels],
        index=short_labels,
    )
    dist_df = pd.DataFrame(dist_matrix, index=short_labels, columns=short_labels)

    condensed = squareform(dist_matrix, checks=False)
    row_linkage = scipy_linkage(condensed, method="average")

    g = sns.clustermap(
        dist_df,
        cmap="RdYlGn_r",
        figsize=(max(12, n * 0.55), max(10, n * 0.5)),
        row_colors=row_colors,
        col_colors=row_colors,
        linewidths=0.3,
        linecolor="white",
        vmin=0,
        cbar_kws={"label": "Cosine distance"},
        dendrogram_ratio=0.15,
        row_linkage=row_linkage,
        col_linkage=row_linkage,
    )
    g.ax_heatmap.set_xticklabels(g.ax_heatmap.get_xticklabels(), fontsize=7)
    g.ax_heatmap.set_yticklabels(g.ax_heatmap.get_yticklabels(), fontsize=7)

    reordered_labels = [t.get_text() for t in g.ax_heatmap.get_xticklabels()]
    for tick, label in zip(
        g.ax_heatmap.get_xticklabels(), reordered_labels, strict=False
    ):
        tick.set_color("#1f77b4" if "\nb" in label else "#ff7f0e")
    for tick, label in zip(
        g.ax_heatmap.get_yticklabels(), reordered_labels, strict=False
    ):
        tick.set_color("#1f77b4" if "\nb" in label else "#ff7f0e")

    book_patch = mpatches.Patch(color="#1f77b4", label="book")
    music_patch = mpatches.Patch(color="#ff7f0e", label="music")
    g.ax_heatmap.legend(
        handles=[book_patch, music_patch],
        loc="upper left",
        bbox_to_anchor=(1.25, 1.1),
        fontsize=8,
    )
    g.fig.suptitle(
        "Hierarchical clustermap of GTL x item_type centroids\n"
        "(green = close  |  red = far  |  color = item type)",
        y=1.01,
        fontsize=11,
    )

    if output_path:
        g.fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
    else:
        plt.show()
    plt.close(g.fig)


def main() -> None:
    """Parse CLI arguments and render the hierarchical clustermap."""
    parser = argparse.ArgumentParser(
        description="Hierarchical clustermap of GTL x item_type centroids."
    )
    parser.add_argument("parquet_path", help="Path to the embeddings parquet file.")
    parser.add_argument(
        "--raw-data",
        "-r",
        required=True,
        help="Raw input parquet or directory to join item_type and raw_gtl_id.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Save plot to this path (PNG). If omitted, display interactively.",
    )
    args = parser.parse_args()

    df = load_embeddings(args.parquet_path, args.raw_data)
    centroids_df = compute_centroids(df)

    print("Generating clustermap…")
    plot_clustermap(centroids_df, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
