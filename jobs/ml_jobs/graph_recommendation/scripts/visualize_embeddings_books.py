"""Visualize book-only embeddings in 2D using PCA, colored by GTL level-1."""

from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA


def load_embeddings(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} embeddings")
    print(f"Columns: {df.columns.tolist()}")
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


def plot(
    df: pd.DataFrame,
    coords: np.ndarray,
    output_path: str | None,
    gtl_level: int = 1,
) -> None:
    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    # Derive GTL level-1 (first 2 digits + '000000') or level-2 from gtl_id
    # gtl_id format: '01020000' → level-1 = '01000000', level-2 = '01020000'
    df["gtl_id_clean"] = df["gtl_id"].astype(str).str.strip()

    if gtl_level == 1:
        # Keep only the first 2 digits, zero-pad to 8 chars
        df["gtl_group"] = df["gtl_id_clean"].str[:2] + "000000"
        title_suffix = "GTL level 1 (first 2 digits)"
    else:
        # Use full level-2 code (ends in '0000')
        df["gtl_group"] = df["gtl_id_clean"].apply(
            lambda g: g if g.endswith("0000") and g != "00000000" else g[:4] + "0000"
        )
        title_suffix = "GTL level 2"

    # Filter out unknown / catch-all GTL
    df_plot = df[
        df["gtl_id_clean"].notna()
        & (df["gtl_id_clean"] != "00000000")
        & (df["gtl_id_clean"] != "nan")
        & (df["gtl_id_clean"] != "None")
    ].copy()

    gtl_groups = sorted(df_plot["gtl_group"].unique())
    n = max(len(gtl_groups), 1)

    # Use tab20 for ≤20 groups, else hsv
    if n <= 20:
        cmap = plt.colormaps["tab20"].resampled(n)
    else:
        cmap = plt.colormaps["hsv"].resampled(n)

    gtl_color = {g: cmap(i) for i, g in enumerate(gtl_groups)}

    fig, ax = plt.subplots(figsize=(14, 10))

    for gtl in gtl_groups:
        mask = df_plot["gtl_group"] == gtl
        subset = df_plot[mask]
        if subset.empty:
            continue
        ax.scatter(
            subset["x"],
            subset["y"],
            c=[gtl_color[gtl]],
            marker="o",
            s=12,
            alpha=0.5,
            linewidths=0,
            label=gtl,
        )

    ax.legend(
        title=title_suffix,
        loc="upper left",
        bbox_to_anchor=(1.01, 1),
        fontsize=7,
        title_fontsize=8,
        markerscale=1.5,
    )

    ax.set_title(f"PCA 2D — book embeddings colored by {title_suffix}")
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved to {output_path}")
    else:
        plt.show()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Visualize book-only embeddings with PCA."
    )
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
    parser.add_argument(
        "--gtl-level",
        type=int,
        default=1,
        choices=[1, 2],
        help="GTL granularity level used for coloring: 1 (broad) or 2 (fine). Default: 1.",
    )
    args = parser.parse_args()

    df = load_embeddings(args.parquet_path)

    if args.nrows and len(df) > args.nrows:
        df = df.sample(args.nrows, random_state=42)
        print(f"Subsampled to {len(df)} rows")

    embeddings = np.stack(df["embedding"].values)
    coords = reduce_pca(embeddings)
    plot(df, coords, args.output, gtl_level=args.gtl_level)


if __name__ == "__main__":
    main()
