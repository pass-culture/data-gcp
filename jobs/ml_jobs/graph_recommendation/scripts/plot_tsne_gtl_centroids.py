"""
t-SNE projection of GTL x item_type cluster centroids.

For each (gtl_id, item_type) group, a centroid embedding is computed.
The centroids are then projected to 2D via t-SNE (cosine metric).
Dashed grey lines connect book/music pairs that share the same GTL code:
a longer line indicates better separation between the two item types.

Usage:
    uv run python scripts/plot_tsne_gtl_centroids.py results/embeddings.parquet \\
        --raw-data   data/raw_input/book_music_catalog \\
        --output     results/tsne_centroids.png
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import numpy as np
from plot_utils import (
    ITEM_TYPE_COLORS,
    ITEM_TYPE_MARKERS,
    compute_centroids,
    load_embeddings,
    save_or_show,
)
from sklearn.manifold import TSNE

if TYPE_CHECKING:
    import pandas as pd


def plot_tsne_centroids(centroids_df: pd.DataFrame, output_path: str | None) -> None:
    """Project cluster centroids to 2D with t-SNE and render the scatter plot.

    Dashed grey lines connect book/music pairs that share the same GTL code.
    A longer line indicates better separation between the two item types.

    Args:
        centroids_df: Output of compute_centroids().
        output_path:  Save path (PNG). Displays interactively when None.
    """
    n = len(centroids_df)
    centroid_matrix = np.stack(centroids_df["centroid"].values)

    tsne = TSNE(
        n_components=2,
        perplexity=min(5, n - 1),
        random_state=42,
        max_iter=2000,
        metric="cosine",
    )
    coords = tsne.fit_transform(centroid_matrix)

    centroids_df = centroids_df.copy()
    centroids_df["x"] = coords[:, 0]
    centroids_df["y"] = coords[:, 1]

    fig, ax = plt.subplots(figsize=(13, 9))

    # Dashed lines connecting same-GTL book/music pairs
    for gtl_id in centroids_df["gtl_id"].unique():
        pair = centroids_df[centroids_df["gtl_id"] == gtl_id]
        if len(pair) == 2:
            ax.plot(
                pair["x"].values,
                pair["y"].values,
                color="grey",
                linewidth=0.8,
                linestyle="--",
                alpha=0.5,
                zorder=1,
            )

    # Scatter one layer per item type
    for item_type, group in centroids_df.groupby("item_type"):
        ax.scatter(
            group["x"],
            group["y"],
            c=ITEM_TYPE_COLORS.get(item_type, "green"),
            marker=ITEM_TYPE_MARKERS.get(item_type, "s"),
            s=120,
            zorder=3,
            label=item_type,
            edgecolors="white",
            linewidths=0.5,
        )

    # Label each centroid point
    for _, row in centroids_df.iterrows():
        ax.annotate(
            f"{row['gtl_id']}\n({'b' if row['item_type'] == 'book' else 'm'})",
            (row["x"], row["y"]),
            textcoords="offset points",
            xytext=(5, 5),
            fontsize=6.5,
            color=ITEM_TYPE_COLORS.get(row["item_type"], "black"),
        )

    ax.legend(title="Item type", fontsize=9)
    ax.set_title(
        "t-SNE of GTL x item_type centroids\n"
        "(dashed lines connect same-GTL book/music pairs"
        " — longer line = better separation)",
        fontsize=11,
    )
    ax.set_xlabel("t-SNE 1")
    ax.set_ylabel("t-SNE 2")
    plt.tight_layout()

    save_or_show(output_path)
    plt.close(fig)


def main() -> None:
    """Parse CLI arguments and render the t-SNE centroid plot."""
    parser = argparse.ArgumentParser(
        description="t-SNE projection of GTL x item_type cluster centroids."
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

    df = load_embeddings(args.parquet_path, args.raw_data, filter_gtl_level2=True)
    centroids_df = compute_centroids(df)

    print("Generating t-SNE of centroids…")
    plot_tsne_centroids(centroids_df, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
