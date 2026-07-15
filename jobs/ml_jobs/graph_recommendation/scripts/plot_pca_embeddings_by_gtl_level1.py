"""
PCA 2D — colored by item_type x GTL level-1, shaped by item_type.

Encoding rules:
  - Same GTL level-1 + same item_type  → same color and shape
  - Same GTL level-1 + different item_type → same hue, different shape (○ / ▲)
  - Different GTL level-1               → different color

Usage:
    uv run python scripts/plot_pca_embeddings_by_gtl_level1.py \\
        --embeddings results/embeddings.parquet \\
        --raw-data   data/raw_input/dataset.parquet \\
        --output     results/pca_gtl_level1.png
"""

from __future__ import annotations

import argparse

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

UNKNOWN_COLOR = (0.7, 0.7, 0.7, 0.3)


def extract_gtl_level1(gtl_id: str) -> str:
    """Return the GTL level-1 code for a given raw GTL identifier.

    GTL level-1 is defined as the first 2 digits padded with six zeros.
    Returns 'unknown' for invalid, null, or catch-all values.

    Examples:
        >>> extract_gtl_level1("01020304")
        '01000000'
        >>> extract_gtl_level1("00000000")
        'unknown'
        >>> extract_gtl_level1("nan")
        'unknown'
    """
    cleaned = str(gtl_id).strip()
    if len(cleaned) >= 2 and cleaned not in ("nan", "None", "00000000"):
        return cleaned[:2] + "000000"
    return "unknown"


def main() -> None:
    """Parse CLI arguments, run PCA, and render the GTL level-1 colored scatter plot."""
    parser = argparse.ArgumentParser(
        description="PCA 2D colored by item_type x GTL level-1."
    )
    add_embedding_args(parser)
    add_raw_data_arg(
        parser, help="Raw input parquet or directory to join item_type and raw_gtl_id."
    )
    add_output_arg(parser)
    add_nrows_arg(parser)
    args = parser.parse_args()

    parquet_path = resolve_parquet_path(args, parser)

    # --- Load embeddings and join item type + GTL from raw data ---
    df = load_embeddings(parquet_path, args.raw_data)
    df = subsample_df(df, args.nrows)

    # --- PCA dimensionality reduction ---
    coords, explained = run_pca_2d(df)

    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]
    df["gtl_l1"] = df["gtl_id"].apply(extract_gtl_level1)

    # --- Build color palette: one color per GTL level-1 code ---
    gtl_l1_values = sorted(v for v in df["gtl_l1"].unique() if v != "unknown")
    n_gtl = len(gtl_l1_values)
    colormap = plt.colormaps["tab20" if n_gtl <= 20 else "hsv"].resampled(max(n_gtl, 1))
    gtl_color = {gtl: colormap(i) for i, gtl in enumerate(gtl_l1_values)}
    gtl_color["unknown"] = UNKNOWN_COLOR

    item_types = sorted(df["item_type"].unique())

    # --- Scatter plot: one layer per (GTL level-1, item type) ---
    _fig, ax = plt.subplots(figsize=(14, 10))

    for gtl in gtl_l1_values:
        for item_type in item_types:
            subset = df[(df["gtl_l1"] == gtl) & (df["item_type"] == item_type)]
            if subset.empty:
                continue
            ax.scatter(
                subset["x"],
                subset["y"],
                c=[gtl_color[gtl]],
                marker=ITEM_TYPE_MARKERS.get(item_type, DEFAULT_MARKER),
                s=14,
                alpha=0.55,
                linewidths=0,
            )

    # --- Legends ---
    handles_gtl = build_gtl_legend_handles(gtl_l1_values, gtl_color, markersize=9)
    handles_type = build_item_type_legend_handles(item_types, markersize=9)

    leg1 = ax.legend(
        handles=handles_gtl,
        title="GTL level 1",
        loc="upper left",
        bbox_to_anchor=(1.01, 1),
        fontsize=7,
        title_fontsize=8,
    )
    ax.add_artist(leg1)
    ax.legend(
        handles=handles_type,
        title="Item type  (○=book  ▲=music)",
        loc="upper left",
        bbox_to_anchor=(1.01, 0.28),
        fontsize=8,
        title_fontsize=8,
    )

    ax.set_title(
        f"PCA 2D — colored by GTL level-1, shaped by item type\n"
        f"(PC1={explained[0]:.1%}, PC2={explained[1]:.1%} explained variance)",
        fontsize=12,
    )
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    plt.tight_layout()

    save_or_show(args.output)


if __name__ == "__main__":
    main()
