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
from plot_utils import (
    ITEM_TYPE_COLORS,
    ITEM_TYPE_MARKERS,
    add_embedding_args,
    add_nrows_arg,
    add_output_arg,
    add_raw_data_arg,
    load_embeddings,
    resolve_parquet_path,
    run_pca_2d,
    save_or_show,
    subsample_df,
)

ITEM_TYPE_ALPHAS = {"book": 0.35, "music": 0.45}


def main() -> None:
    """Parse CLI arguments, run PCA, and render the book vs music separation plot."""
    parser = argparse.ArgumentParser(description="Visualize book vs music separation.")
    add_embedding_args(parser)
    add_raw_data_arg(parser, help="Raw input parquet or directory to join item_type.")
    add_output_arg(parser)
    add_nrows_arg(parser)
    args = parser.parse_args()

    parquet_path = resolve_parquet_path(args, parser)

    # --- Load embeddings and join item type from raw data ---
    df = load_embeddings(parquet_path, args.raw_data, include_gtl=False)
    df = subsample_df(df, args.nrows)

    # --- PCA dimensionality reduction ---
    coords, explained = run_pca_2d(df)

    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    # --- Scatter plot: one layer per item type ---
    _fig, ax = plt.subplots(figsize=(12, 8))

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

    save_or_show(args.output)


if __name__ == "__main__":
    main()
