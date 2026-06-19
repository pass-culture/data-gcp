"""
Compute pairwise cosine distances between GTL x item_type cluster centroids.

For each (gtl_id, item_type) pair, the centroid of all embeddings in that group
is computed. Pairwise cosine distances between centroids are then output as:
  - a heatmap PNG

Usage:
    uv run python scripts/plot_gtl_cluster_distance_heatmap.py \
        results/embeddings.parquet \\
        --raw-data data/raw_input/dataset.parquet \\
        --plot     results/cluster_distances_heatmap.png
"""

from __future__ import annotations

import argparse

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_distances


def load_embeddings(parquet_path: str, raw_data_path: str) -> pd.DataFrame:
    """Load embeddings and join item_type and GTL code from the raw dataset.

    Args:
        parquet_path:   Path to the embeddings parquet file.
        raw_data_path:  Path to the raw parquet file or directory.

    Returns:
        DataFrame with columns: node_ids, embedding, item_id, item_type, gtl_id.
    """
    df = pd.read_parquet(parquet_path)
    raw = pd.read_parquet(raw_data_path).loc[:, ["item_id", "item_type", "raw_gtl_id"]]
    df = (
        df.drop(columns=["gtl_id"], errors="ignore")
        .merge(raw, left_on="node_ids", right_on="item_id", how="left")
        .rename(columns={"raw_gtl_id": "gtl_id"})
    )

    print(f"Loaded {len(df)} embeddings")
    print(df["item_type"].value_counts().to_string())

    return df


def compute_centroids(df: pd.DataFrame) -> pd.DataFrame:
    """Compute one centroid embedding per (gtl_id, item_type) group.

    Only GTL level-2 codes (ending in '0000', excluding '00000000') are kept.

    Args:
        df: DataFrame with columns: embedding, gtl_id, item_type.

    Returns:
        DataFrame with columns: cluster, gtl_id, item_type, n_items, centroid.
    """
    df = df[
        df["gtl_id"].notna()
        & df["gtl_id"].astype(str).str.endswith("0000")
        & (df["gtl_id"].astype(str) != "00000000")
    ].copy()

    embeddings_matrix = np.stack(df["embedding"].values)

    records = []
    for (gtl_id, item_type), group in df.groupby(["gtl_id", "item_type"]):
        positions = [df.index.get_loc(i) for i in group.index]
        centroid = embeddings_matrix[positions].mean(axis=0)
        records.append(
            {
                "cluster": f"{gtl_id}__{item_type}",
                "gtl_id": gtl_id,
                "item_type": item_type,
                "n_items": len(group),
                "centroid": centroid,
            }
        )

    centroids_df = pd.DataFrame(records).sort_values(["gtl_id", "item_type"])
    print(f"\nFound {len(centroids_df)} clusters (gtl_id x item_type)")

    return centroids_df


def compute_distances(centroids_df: pd.DataFrame) -> pd.DataFrame:
    """Build a long-form pairwise cosine distance table between all cluster centroids.

    The diagonal (self-distance) is excluded. Each directed pair (i → j) appears
    as one row, so the table has n*(n-1) rows for n clusters.

    Args:
        centroids_df: Output of compute_centroids().

    Returns:
        DataFrame sorted by (src_cluster, cosine_distance) with columns:
        src_cluster, src_gtl_id, src_item_type, src_n_items,
        dst_cluster, dst_gtl_id, dst_item_type, dst_n_items,
        cosine_distance, same_gtl, same_item_type.
    """
    centroid_matrix = np.stack(centroids_df["centroid"].values)
    dist_matrix = cosine_distances(centroid_matrix)
    n = len(centroids_df)

    rows = []
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            src = centroids_df.iloc[i]
            dst = centroids_df.iloc[j]
            rows.append(
                {
                    "src_cluster": src["cluster"],
                    "src_gtl_id": src["gtl_id"],
                    "src_item_type": src["item_type"],
                    "src_n_items": src["n_items"],
                    "dst_cluster": dst["cluster"],
                    "dst_gtl_id": dst["gtl_id"],
                    "dst_item_type": dst["item_type"],
                    "dst_n_items": dst["n_items"],
                    "cosine_distance": round(dist_matrix[i, j], 6),
                    "same_gtl": src["gtl_id"] == dst["gtl_id"],
                    "same_item_type": src["item_type"] == dst["item_type"],
                }
            )

    return pd.DataFrame(rows).sort_values(["src_cluster", "cosine_distance"])


def print_summary(dist_df: pd.DataFrame) -> None:
    """Print a human-readable summary: top-5 nearest neighbours per cluster,
    then a cross-type separation analysis for clusters sharing the same GTL code.

    Args:
        dist_df: Output of compute_distances().
    """
    print("\n" + "=" * 80)
    print("NEAREST NEIGHBOURS PER CLUSTER (top 5)")
    print("=" * 80)

    for src_cluster, group in dist_df.groupby("src_cluster"):
        top5 = group.nsmallest(5, "cosine_distance")
        print(f"\n▶  {src_cluster}  (n={group['src_n_items'].iloc[0]})")
        for _, row in top5.iterrows():
            tag = ""
            if row["same_gtl"] and not row["same_item_type"]:
                tag = "  ← SAME GTL, DIFFERENT TYPE"
            elif row["same_gtl"] and row["same_item_type"]:
                tag = "  ← SAME GTL, SAME TYPE"
            print(
                f"   {row['cosine_distance']:.4f}  {row['dst_cluster']}"
                f"  (n={row['dst_n_items']}){tag}"
            )

    print("\n" + "=" * 80)
    print("SAME-GTL CROSS-TYPE DISTANCE SUMMARY")
    print("(Are book/music with the same GTL code far apart?)")
    print("=" * 80)

    cross_type = dist_df[dist_df["same_gtl"] & ~dist_df["same_item_type"]].copy()
    if cross_type.empty:
        print("No cross-type same-GTL pairs found.")
        return

    nearest_same_type_distance = (
        dist_df[dist_df["same_item_type"] & ~dist_df["same_gtl"]]
        .groupby("src_cluster")["cosine_distance"]
        .min()
        .rename("nearest_same_type_dist")
    )

    cross_type = cross_type.merge(
        nearest_same_type_distance, left_on="src_cluster", right_index=True, how="left"
    )
    cross_type["ratio"] = (
        cross_type["cosine_distance"] / cross_type["nearest_same_type_dist"]
    )

    print(
        f"\n{'Cluster':<35} {'Cross-GTL dist':>16}"
        f" {'Nearest same-type':>18} {'Ratio':>7}"
    )
    print("-" * 80)
    for _, row in cross_type.sort_values("ratio").iterrows():
        flag = "✗ TOO CLOSE" if row["ratio"] < 1.0 else "✓ well separated"
        print(
            f"{row['src_cluster']:<35} "
            f"{row['cosine_distance']:>16.4f} "
            f"{row['nearest_same_type_dist']:>18.4f} "
            f"{row['ratio']:>7.2f}  {flag}"
        )


def plot_heatmap(
    centroids_df: pd.DataFrame,
    dist_df: pd.DataFrame,
    plot_path: str | None,
) -> None:
    """Plot a square heatmap of pairwise cosine distances between cluster centroids.

    Visual encoding:
      - Color scale: green (close) → red (far)
      - Tick label color: blue = book, orange = music
      - Red border on cell: same GTL code but different item type
      - Black separator line between the book block and the music block

    Args:
        centroids_df: Output of compute_centroids() (used for cluster ordering).
        dist_df:      Output of compute_distances().
        plot_path:    Save path (PNG). Displays interactively when None.
    """
    pivot = dist_df.pivot_table(
        index="src_cluster", columns="dst_cluster", values="cosine_distance"
    )
    for cluster in pivot.columns:
        if cluster in pivot.index:
            pivot.loc[cluster, cluster] = 0.0

    # Sort: books first, then music, alphabetically within each type
    order = centroids_df.sort_values(["item_type", "gtl_id"])["cluster"].tolist()
    order = [c for c in order if c in pivot.index]
    pivot = pivot.loc[order, order]

    labels = pivot.index.tolist()
    n = len(labels)
    matrix = pivot.values.astype(float)
    short_labels = [
        f"{c.split('__')[0]}\n({'b' if 'book' in c else 'm'})" for c in labels
    ]
    label_colors = ["#1f77b4" if "book" in c else "#ff7f0e" for c in labels]

    fig, ax = plt.subplots(figsize=(max(10, n * 0.55), max(8, n * 0.5)))
    im = ax.imshow(matrix, cmap="RdYlGn_r", aspect="auto", vmin=0, vmax=matrix.max())

    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(short_labels, rotation=90, fontsize=7)
    ax.set_yticklabels(short_labels, fontsize=7)

    for tick, color in zip(ax.get_xticklabels(), label_colors, strict=False):
        tick.set_color(color)
    for tick, color in zip(ax.get_yticklabels(), label_colors, strict=False):
        tick.set_color(color)

    # Highlight same-GTL cross-type pairs with a red rectangle
    same_gtl_cross_pairs = dist_df[dist_df["same_gtl"] & ~dist_df["same_item_type"]]
    for _, row in same_gtl_cross_pairs.iterrows():
        if row["src_cluster"] in labels and row["dst_cluster"] in labels:
            i = labels.index(row["src_cluster"])
            j = labels.index(row["dst_cluster"])
            ax.add_patch(
                mpatches.Rectangle(
                    (j - 0.5, i - 0.5),
                    1,
                    1,
                    linewidth=2,
                    edgecolor="red",
                    facecolor="none",
                )
            )

    # Separator line between the book block and the music block
    n_book_clusters = sum(1 for c in labels if "book" in c)
    if 0 < n_book_clusters < n:
        ax.axhline(n_book_clusters - 0.5, color="black", linewidth=1.5)
        ax.axvline(n_book_clusters - 0.5, color="black", linewidth=1.5)

    plt.colorbar(im, ax=ax, label="Cosine distance", shrink=0.6)

    book_patch = mpatches.Patch(color="#1f77b4", label="book")
    music_patch = mpatches.Patch(color="#ff7f0e", label="music")
    red_patch = mpatches.Patch(
        edgecolor="red", facecolor="none", linewidth=2, label="same GTL, cross-type"
    )
    ax.legend(
        handles=[book_patch, music_patch, red_patch],
        loc="upper left",
        bbox_to_anchor=(1.15, 1),
        fontsize=8,
    )

    ax.set_title(
        "Pairwise cosine distance between GTL x item_type cluster centroids\n"
        "(green = close, red = far  |  red border = same GTL code, different type)",
        fontsize=10,
    )
    plt.tight_layout()

    if plot_path:
        plt.savefig(plot_path, dpi=150, bbox_inches="tight")
        print(f"Heatmap saved to {plot_path}")
    else:
        plt.show()


def main() -> None:
    """Parse CLI arguments and run the full cluster distance pipeline."""
    parser = argparse.ArgumentParser(
        description=(
            "Compute pairwise distances between GTL+item_type cluster centroids."
        )
    )
    parser.add_argument("parquet_path", help="Path to the embeddings parquet file.")
    parser.add_argument(
        "--raw-data",
        "-r",
        required=True,
        help="Raw input parquet or directory to join item_type.",
    )
    parser.add_argument(
        "--plot",
        "-p",
        default=None,
        help="Save heatmap PNG to this path. If omitted, display interactively.",
    )
    args = parser.parse_args()

    df = load_embeddings(args.parquet_path, args.raw_data)
    centroids_df = compute_centroids(df)
    dist_df = compute_distances(centroids_df)

    print_summary(dist_df)
    plot_heatmap(centroids_df, dist_df, args.plot)


if __name__ == "__main__":
    main()
