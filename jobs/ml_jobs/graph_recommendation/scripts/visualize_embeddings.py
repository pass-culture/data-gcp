"""Visualize embeddings in 2D using PCA, colored by GTL and item_type."""

from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA


def load_embeddings(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} embeddings")
    print(df["item_type"].value_counts().to_string())
    print(f"GTL sample: {df['gtl_id'].dropna().unique()[:5].tolist()}")
    return df


def reduce_pca(embeddings: np.ndarray) -> np.ndarray:
    pca = PCA(n_components=2, random_state=42)
    reduced = pca.fit_transform(embeddings)
    explained = pca.explained_variance_ratio_
    print(
        f"PCA explained variance: PC1={explained[0]:.1%}, PC2={explained[1]:.1%} "
        f"(total={sum(explained):.1%})"
    )
    return reduced


def plot(df: pd.DataFrame, coords: np.ndarray, output_path: str | None) -> None:
    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    # Keep only rows with a valid GTL (level-2 codes ending in '0000')
    df_plot = df[
        df["gtl_id"].notna()
        & df["gtl_id"].str.endswith("0000")
        & (df["gtl_id"] != "00000000")
    ].copy()

    gtl_ids = sorted(df_plot["gtl_id"].unique())
    cmap = plt.colormaps["tab20"].resampled(max(len(gtl_ids), 1))
    gtl_color = {gtl: cmap(i) for i, gtl in enumerate(gtl_ids)}

    item_types = df_plot["item_type"].unique()
    markers = {"book": "o", "music": "^"}
    default_marker = "s"

    fig, ax = plt.subplots(figsize=(14, 10))

    for gtl in gtl_ids:
        for itype in item_types:
            mask = (df_plot["gtl_id"] == gtl) & (df_plot["item_type"] == itype)
            subset = df_plot[mask]
            if subset.empty:
                continue
            marker = markers.get(itype, default_marker)
            label = f"{gtl} ({itype})" if itype == item_types[0] else None
            ax.scatter(
                subset["x"],
                subset["y"],
                c=[gtl_color[gtl]],
                marker=marker,
                s=18,
                alpha=0.6,
                linewidths=0,
                label=label,
            )

    # Legend: GTL colors
    handles_gtl = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=gtl_color[g],
            markersize=8,
            label=g,
        )
        for g in gtl_ids
    ]
    # Legend: item_type markers
    handles_type = [
        plt.Line2D(
            [0],
            [0],
            marker=markers.get(t, default_marker),
            color="grey",
            markersize=8,
            linestyle="None",
            label=t,
        )
        for t in item_types
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
    parser = argparse.ArgumentParser(description="Visualize embeddings with PCA.")
    parser.add_argument("parquet_path", help="Path to the embeddings parquet file.")
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

    df = load_embeddings(args.parquet_path)

    if args.nrows and len(df) > args.nrows:
        df = df.sample(args.nrows, random_state=42)
        print(f"Subsampled to {len(df)} rows")

    embeddings = np.stack(df["embedding"].values)
    coords = reduce_pca(embeddings)
    plot(df, coords, args.output)


if __name__ == "__main__":
    main()
