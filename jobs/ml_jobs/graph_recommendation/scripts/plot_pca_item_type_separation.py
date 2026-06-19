"""
PCA 2D — visualize book vs music separation.

Reduces item embeddings to 2 principal components and colors each point
by item type (book / music). Centroids are drawn as stars.

Usage:
    uv run python scripts/plot_pca_item_type_separation.py \\
        --embeddings results/embeddings.parquet \\
        --raw-data   data/raw_input/dataset.parquet \\
        --output     results/pca_book_vs_music.png
"""

from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

ITEM_TYPE_COLORS = {"book": "#1f77b4", "music": "#ff7f0e"}
ITEM_TYPE_MARKERS = {"book": "o", "music": "^"}
ITEM_TYPE_ALPHAS = {"book": 0.35, "music": 0.45}


def main() -> None:
    """Parse CLI arguments, run PCA, and render the book vs music separation plot."""
    parser = argparse.ArgumentParser(description="Visualize book vs music separation.")
    parser.add_argument("parquet_path", nargs="?", default=None)
    parser.add_argument("--embeddings", "-e", default=None)
    parser.add_argument(
        "--raw-data",
        "-r",
        required=True,
        help="Raw input parquet or directory to join item_type.",
    )
    parser.add_argument("--output", "-o", default=None)
    parser.add_argument("--nrows", type=int, default=None)
    args = parser.parse_args()

    parquet_path = args.embeddings or args.parquet_path
    if not parquet_path:
        parser.error(
            "Provide embeddings path as positional argument or via --embeddings"
        )

    # --- Load embeddings and join item type from raw data ---
    df = pd.read_parquet(parquet_path)
    df = df.merge(
        pd.read_parquet(args.raw_data).loc[:, ["item_id", "item_type"]],
        left_on="node_ids",
        right_on="item_id",
        how="left",
    )
    print(f"Loaded {len(df)} embeddings")
    print(df["item_type"].value_counts().to_string())

    if args.nrows and len(df) > args.nrows:
        df = df.sample(args.nrows, random_state=42)
        print(f"Subsampled to {len(df)} rows")

    # --- PCA dimensionality reduction ---
    embeddings = np.stack(df["embedding"].values)
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(embeddings)
    explained = pca.explained_variance_ratio_
    print(f"PCA variance: PC1={explained[0]:.1%}, PC2={explained[1]:.1%}")

    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    # --- Scatter plot: one layer per item type ---
    fig, ax = plt.subplots(figsize=(12, 8))

    for item_type, group in df.groupby("item_type"):
        ax.scatter(
            group["x"],
            group["y"],
            c=ITEM_TYPE_COLORS.get(item_type, "grey"),
            marker=ITEM_TYPE_MARKERS.get(item_type, "s"),
            s=10,
            alpha=ITEM_TYPE_ALPHAS.get(item_type, 0.4),
            linewidths=0,
            label=f"{item_type} (n={len(group)})",
        )

    # --- Centroid markers ---
    for item_type, group in df.groupby("item_type"):
        cx, cy = group["x"].mean(), group["y"].mean()
        ax.scatter(
            cx,
            cy,
            c=ITEM_TYPE_COLORS.get(item_type, "grey"),
            marker="*",
            s=300,
            edgecolors="black",
            linewidths=0.8,
            zorder=5,
        )
        ax.annotate(
            f"{item_type} centroid",
            (cx, cy),
            textcoords="offset points",
            xytext=(8, 4),
            fontsize=9,
            color=ITEM_TYPE_COLORS.get(item_type, "grey"),
            fontweight="bold",
        )

    ax.legend(title="Item type", fontsize=10)
    ax.set_title(
        f"PCA 2D — Book vs Music separation\n"
        f"(PC1={explained[0]:.1%}, PC2={explained[1]:.1%} explained variance)",
        fontsize=12,
    )
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    plt.tight_layout()

    if args.output:
        plt.savefig(args.output, dpi=150, bbox_inches="tight")
        print(f"Saved to {args.output}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
