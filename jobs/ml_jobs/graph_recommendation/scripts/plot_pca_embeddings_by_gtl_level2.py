"""
Visualize embeddings in 2D using PCA, colored by GTL level-2 and item type.

Each point represents one item. Color encodes the GTL level-2 genre code;
shape encodes the item type (○ = book, ▲ = music).
Only items with a valid GTL level-2 code are plotted.

Usage:
    uv run python scripts/plot_pca_embeddings_by_gtl_level2.py \\
        --embeddings results/embeddings.parquet \\
        --raw-data   data/raw_input/dataset.parquet \\
        --output     results/pca_gtl_level2_multitype.png
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
from plot_utils import (
    DEFAULT_MARKER,
    ITEM_TYPE_MARKERS,
    add_embedding_args,
    add_nrows_arg,
    add_output_arg,
    add_raw_data_arg,
    build_gtl_legend_handles,
    build_item_type_legend_handles,
    load_embeddings,
    resolve_parquet_path,
    run_pca_2d,
    save_or_show,
    subsample_df,
)

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd


def plot_embeddings(
    df: pd.DataFrame, coords: np.ndarray, output_path: str | None
) -> None:
    """Render a 2D scatter plot of embeddings colored by GTL level-2 and shaped by
    item type.

    Only rows with a valid GTL level-2 code (ending in '0000', excluding
    '00000000') are shown.

    Args:
        df:           DataFrame with item_type and gtl_id columns.
        coords:       2D PCA coordinates aligned with df rows.
        output_path:  Save path (PNG/PDF). Displays interactively when None.
    """
    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    # Keep only valid GTL level-2 codes
    df_plot = df[
        df["gtl_id"].notna()
        & df["gtl_id"].str.endswith("0000")
        & (df["gtl_id"] != "00000000")
    ].copy()

    gtl_ids = sorted(df_plot["gtl_id"].unique())
    colormap = plt.colormaps["tab20"].resampled(max(len(gtl_ids), 1))
    gtl_color = {gtl: colormap(i) for i, gtl in enumerate(gtl_ids)}

    item_types = df_plot["item_type"].unique()

    # --- Scatter plot: one layer per (GTL, item type) ---
    _fig, ax = plt.subplots(figsize=(14, 10))

    for gtl in gtl_ids:
        for item_type in item_types:
            subset = df_plot[
                (df_plot["gtl_id"] == gtl) & (df_plot["item_type"] == item_type)
            ]
            if subset.empty:
                continue
            label = f"{gtl} ({item_type})" if item_type == item_types[0] else None
            ax.scatter(
                subset["x"],
                subset["y"],
                c=[gtl_color[gtl]],
                marker=ITEM_TYPE_MARKERS.get(item_type, DEFAULT_MARKER),
                s=18,
                alpha=0.6,
                linewidths=0,
                label=label,
            )

    # --- Legends ---
    handles_gtl = build_gtl_legend_handles(gtl_ids, gtl_color, markersize=8)
    handles_type = build_item_type_legend_handles(list(item_types), markersize=8)

    leg1 = ax.legend(
        handles=handles_gtl,
        title="GTL (level 2)",
        loc="upper left",
        bbox_to_anchor=(1.01, 1),
        fontsize=7,
        title_fontsize=8,
    )
    ax.add_artist(leg1)
    ax.legend(
        handles=handles_type,
        title="Item type",
        loc="upper left",
        bbox_to_anchor=(1.01, 0.35),
        fontsize=8,
        title_fontsize=8,
    )

    ax.set_title("PCA 2D — embeddings colored by GTL (shape = item type)")
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    plt.tight_layout()

    save_or_show(output_path)


def main() -> None:
    """Parse CLI arguments, run PCA, and render the GTL level-2 scatter plot."""
    parser = argparse.ArgumentParser(description="Visualize embeddings with PCA.")
    add_embedding_args(parser)
    add_raw_data_arg(
        parser, help="Raw input parquet or directory to join item_type and raw_gtl_id."
    )
    add_output_arg(
        parser,
        help="Save plot to this path (PNG/PDF). If omitted, display interactively.",
    )
    add_nrows_arg(parser)
    args = parser.parse_args()

    parquet_path = resolve_parquet_path(args, parser)

    df = load_embeddings(parquet_path, args.raw_data)
    print(f"GTL sample: {df['gtl_id'].dropna().unique()[:5].tolist()}")

    df = subsample_df(df, args.nrows)

    coords, _explained = run_pca_2d(df)
    plot_embeddings(df, coords, args.output)


if __name__ == "__main__":
    main()
