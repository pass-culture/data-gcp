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
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

ITEM_TYPE_MARKERS = {"book": "o", "music": "^"}
DEFAULT_MARKER = "s"
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
    parser.add_argument("parquet_path", nargs="?", default=None)
    parser.add_argument("--embeddings", "-e", default=None)
    parser.add_argument(
        "--raw-data",
        "-r",
        required=True,
        help="Raw input parquet or directory to join item_type and raw_gtl_id.",
    )
    parser.add_argument("--output", "-o", default=None)
    parser.add_argument("--nrows", type=int, default=None)
    args = parser.parse_args()

    parquet_path = args.embeddings or args.parquet_path
    if not parquet_path:
        parser.error(
            "Provide embeddings path as positional argument or via --embeddings"
        )

    # --- Load embeddings and join item type + GTL from raw data ---
    raw = pd.read_parquet(args.raw_data).loc[:, ["item_id", "item_type", "raw_gtl_id"]]
    df = (
        pd.read_parquet(parquet_path)
        .drop(columns=["gtl_id"], errors="ignore")
        .merge(raw, left_on="node_ids", right_on="item_id", how="left")
        .rename(columns={"raw_gtl_id": "gtl_id"})
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
    df["gtl_l1"] = df["gtl_id"].apply(extract_gtl_level1)

    # --- Build color palette: one color per GTL level-1 code ---
    gtl_l1_values = sorted(v for v in df["gtl_l1"].unique() if v != "unknown")
    n_gtl = len(gtl_l1_values)
    colormap = plt.colormaps["tab20" if n_gtl <= 20 else "hsv"].resampled(max(n_gtl, 1))
    gtl_color = {gtl: colormap(i) for i, gtl in enumerate(gtl_l1_values)}
    gtl_color["unknown"] = UNKNOWN_COLOR

    item_types = sorted(df["item_type"].unique())

    # --- Scatter plot: one layer per (GTL level-1, item type) ---
    fig, ax = plt.subplots(figsize=(14, 10))

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

    # --- Legend: GTL level-1 colors ---
    handles_gtl = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=gtl_color[gtl],
            markersize=9,
            label=gtl,
        )
        for gtl in gtl_l1_values
    ]

    # --- Legend: item type markers ---
    handles_type = [
        plt.Line2D(
            [0],
            [0],
            marker=ITEM_TYPE_MARKERS.get(item_type, DEFAULT_MARKER),
            color="grey",
            markersize=9,
            linestyle="None",
            label=item_type,
        )
        for item_type in item_types
    ]

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

    if args.output:
        plt.savefig(args.output, dpi=150, bbox_inches="tight")
        print(f"Saved to {args.output}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
