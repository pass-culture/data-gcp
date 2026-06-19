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

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

ITEM_TYPE_MARKERS = {"book": "o", "music": "^"}
DEFAULT_MARKER = "s"


def load_embeddings(parquet_path: str, raw_data_path: str) -> pd.DataFrame:
    """Load embeddings and join item_type and GTL code from the raw dataset.

    Args:
        parquet_path:   Path to the embeddings parquet file
                        (columns: node_ids, embedding).
        raw_data_path:  Path to the raw parquet file or directory (columns: item_id,
                        item_type, raw_gtl_id).

    Returns:
        DataFrame with columns: node_ids, embedding, item_id, item_type, gtl_id.
    """
    raw = pd.read_parquet(raw_data_path).loc[:, ["item_id", "item_type", "raw_gtl_id"]]
    df = (
        pd.read_parquet(parquet_path)
        .drop(columns=["gtl_id"], errors="ignore")
        .merge(raw, left_on="node_ids", right_on="item_id", how="left")
        .rename(columns={"raw_gtl_id": "gtl_id"})
    )

    print(f"Loaded {len(df)} embeddings")
    print(df["item_type"].value_counts().to_string())
    print(f"GTL sample: {df['gtl_id'].dropna().unique()[:5].tolist()}")

    return df


def reduce_to_2d_pca(embeddings: np.ndarray) -> np.ndarray:
    """Fit a 2-component PCA on the embedding matrix and return the 2D coordinates.

    Args:
        embeddings: 2D array of shape (n_items, embedding_dim).

    Returns:
        2D array of shape (n_items, 2) containing the projected coordinates.
    """
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(embeddings)
    explained = pca.explained_variance_ratio_
    print(
        f"PCA explained variance: PC1={explained[0]:.1%}, PC2={explained[1]:.1%} "
        f"(total={sum(explained):.1%})"
    )
    return coords


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
    fig, ax = plt.subplots(figsize=(14, 10))

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

    # --- Legend: GTL level-2 colors ---
    handles_gtl = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=gtl_color[gtl],
            markersize=8,
            label=gtl,
        )
        for gtl in gtl_ids
    ]

    # --- Legend: item type markers ---
    handles_type = [
        plt.Line2D(
            [0],
            [0],
            marker=ITEM_TYPE_MARKERS.get(item_type, DEFAULT_MARKER),
            color="grey",
            markersize=8,
            linestyle="None",
            label=item_type,
        )
        for item_type in item_types
    ]

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

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved to {output_path}")
    else:
        plt.show()


def main() -> None:
    """Parse CLI arguments, run PCA, and render the GTL level-2 scatter plot."""
    parser = argparse.ArgumentParser(description="Visualize embeddings with PCA.")
    parser.add_argument("parquet_path", nargs="?", default=None)
    parser.add_argument("--embeddings", "-e", default=None)
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
        help="Save plot to this path (PNG/PDF). If omitted, display interactively.",
    )
    parser.add_argument(
        "--nrows",
        type=int,
        default=None,
        help="Subsample N rows before plotting (faster for large files).",
    )
    args = parser.parse_args()

    parquet_path = args.embeddings or args.parquet_path
    if not parquet_path:
        parser.error(
            "Provide embeddings path as positional argument or via --embeddings"
        )

    df = load_embeddings(parquet_path, args.raw_data)

    if args.nrows and len(df) > args.nrows:
        df = df.sample(args.nrows, random_state=42)
        print(f"Subsampled to {len(df)} rows")

    embeddings = np.stack(df["embedding"].values)
    coords = reduce_to_2d_pca(embeddings)
    plot_embeddings(df, coords, args.output)


if __name__ == "__main__":
    main()
