"""
PCA 2D — highlight items linked to a cross-type artist.

A "cross-type artist" is an artist who has both book and music items.
Items belonging to such artists are plotted in vivid colors (foreground);
all other items appear as pale background points.

Color / shape encoding:
  - Book  linked to a cross-type artist → dark blue  (●)
  - Music linked to a cross-type artist → dark red   (▲)
  - Book  without a cross-type artist   → pale blue  (●)
  - Music without a cross-type artist   → pale red   (▲)

Usage:
    uv run python scripts/plot_pca_cross_type_artist_items.py \\
        --embeddings results/embeddings.parquet \\
        --raw-data   data/raw_input/input_with_prefix.parquet \\
        --output     results/pca_cross_type_artist.png
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
from plot_utils import (
    add_embedding_args,
    add_nrows_arg,
    add_output_arg,
    add_raw_data_arg,
    load_embeddings,
    load_parquet_or_dir,
    resolve_parquet_path,
    run_pca_2d,
    safe_resolve_path,
    save_or_show,
    subsample_df,
)

if TYPE_CHECKING:
    import pandas as pd

# (color, marker, legend label)
STYLE_CROSS_TYPE = {
    "book": ("#1565C0", "o", "book  — shared artist ✓"),
    "music": ("#E53935", "^", "music — shared artist ✓"),
}
STYLE_BACKGROUND = {
    "book": ("#90CAF9", "o", "book  — no shared artist"),
    "music": ("#FFCDD2", "^", "music — no shared artist"),
}


def load_artist_mapping(raw_path: str) -> pd.DataFrame:
    """Load the item_id → (item_type, artist_id) mapping from raw parquet files.

    Accepts either a single parquet file or a directory tree (recursive scan).

    Args:
        raw_path: Path to a parquet file or a directory containing parquet files.

    Returns:
        Deduplicated DataFrame with columns: item_id, item_type, artist_id.

    Raises:
        ValueError: If the path is invalid or contains illegal characters.
        FileNotFoundError: If raw_path is a directory with no parquet files.
    """
    df = load_parquet_or_dir(raw_path)
    return df[["item_id", "item_type", "artist_id"]].drop_duplicates(subset=["item_id"])


def find_cross_type_item_ids(artist_mapping: pd.DataFrame) -> set[str]:
    """Return the set of item_ids whose artist appears in both book and music items.

    Args:
        artist_mapping: Output of load_artist_mapping().

    Returns:
        Set of item_id strings that belong to a cross-type artist.
    """
    items_with_artist = artist_mapping[artist_mapping["artist_id"].notna()].copy()
    items_with_artist["artist_id"] = items_with_artist["artist_id"].astype(str)

    artist_type_count = items_with_artist.groupby("artist_id")["item_type"].nunique()
    cross_type_artists = set(artist_type_count[artist_type_count > 1].index)

    cross_type_items = set(
        items_with_artist[items_with_artist["artist_id"].isin(cross_type_artists)][
            "item_id"
        ].astype(str)
    )

    print(f"  Cross-type artists : {len(cross_type_artists)}")
    print(f"  Cross-type items   : {len(cross_type_items)}")

    return cross_type_items


def main() -> None:
    """Parse CLI arguments, run PCA, render the cross-type artist separation plot."""
    parser = argparse.ArgumentParser(
        description="PCA 2D — highlight items linked by cross-type artists."
    )
    add_embedding_args(parser)
    add_raw_data_arg(
        parser,
        help=(
            "Raw input parquet or directory"
            " (used both for item_type join and artist mapping)."
        ),
    )
    add_output_arg(parser)
    add_nrows_arg(parser)
    args = parser.parse_args()

    parquet_path = resolve_parquet_path(args, parser)

    # Validate raw-data path early
    safe_resolve_path(args.raw_data)

    # --- Load embeddings and join item type ---
    emb_df = load_embeddings(parquet_path, args.raw_data, include_gtl=False)

    # --- Identify cross-type items ---
    print("Loading artist mapping from raw data…")
    artist_mapping = load_artist_mapping(args.raw_data)
    cross_type_items = find_cross_type_item_ids(artist_mapping)

    emb_df = subsample_df(emb_df, args.nrows)

    # --- PCA dimensionality reduction ---
    coords, explained = run_pca_2d(emb_df)

    emb_df = emb_df.copy()
    emb_df["x"] = coords[:, 0]
    emb_df["y"] = coords[:, 1]
    emb_df["is_cross"] = emb_df["node_ids"].astype(str).isin(cross_type_items)

    print(f"Plotting {len(emb_df)} items ({emb_df['is_cross'].sum()} cross-type)")

    # --- Plot ---
    _fig, ax = plt.subplots(figsize=(13, 9))
    ax.set_facecolor("#F5F5F5")

    # Background: items without a cross-type artist (transparent, small)
    for item_type, (color, marker, label) in STYLE_BACKGROUND.items():
        subset = emb_df[(emb_df["item_type"] == item_type) & ~emb_df["is_cross"]]
        if subset.empty:
            continue
        ax.scatter(
            subset["x"],
            subset["y"],
            c=color,
            marker=marker,
            s=8,
            alpha=0.25,
            linewidths=0,
            label=f"{label} (n={len(subset)})",
            zorder=1,
        )

    # Foreground: cross-type items (opaque, larger)
    for item_type, (color, marker, label) in STYLE_CROSS_TYPE.items():
        subset = emb_df[(emb_df["item_type"] == item_type) & emb_df["is_cross"]]
        if subset.empty:
            continue
        ax.scatter(
            subset["x"],
            subset["y"],
            c=color,
            marker=marker,
            s=35,
            alpha=1.0,
            linewidths=0,
            label=f"{label} (n={len(subset)})",
            zorder=3,
        )

    # Centroid markers for cross-type items only
    for item_type, (color, *_) in STYLE_CROSS_TYPE.items():
        subset = emb_df[(emb_df["item_type"] == item_type) & emb_df["is_cross"]]
        if subset.empty:
            continue
        cx, cy = subset["x"].mean(), subset["y"].mean()
        ax.scatter(
            cx,
            cy,
            c=color,
            marker="*",
            s=350,
            edgecolors="black",
            linewidths=0.8,
            zorder=6,
        )
        ax.annotate(
            f"{item_type} centroid",
            (cx, cy),
            textcoords="offset points",
            xytext=(8, 4),
            fontsize=8,
            color=color,
            fontweight="bold",
        )

    ax.legend(title="Item type x artist link", fontsize=9, loc="best")
    ax.set_title(
        "PCA 2D — Items with cross-type artist (book ↔ music)\n"
        f"Blue ● = books  |  Red ▲ = music  |  "
        f"PC1={explained[0]:.1%}, PC2={explained[1]:.1%}",
        fontsize=11,
    )
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    plt.tight_layout()

    save_or_show(args.output)


if __name__ == "__main__":
    main()
