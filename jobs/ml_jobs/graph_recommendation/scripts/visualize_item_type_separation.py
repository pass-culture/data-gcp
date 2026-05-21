"""PCA 2D — visualize book vs music separation only."""

from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA


def main() -> None:
    parser = argparse.ArgumentParser(description="Visualize book vs music separation.")
    parser.add_argument("parquet_path")
    parser.add_argument("--output", "-o", default=None)
    parser.add_argument("--nrows", type=int, default=None)
    args = parser.parse_args()

    df = pd.read_parquet(args.parquet_path)
    print(f"Loaded {len(df)} embeddings")
    print(df["item_type"].value_counts().to_string())

    if args.nrows and len(df) > args.nrows:
        df = df.sample(args.nrows, random_state=42)
        print(f"Subsampled to {len(df)} rows")

    embeddings = np.stack(df["embedding"].values)
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(embeddings)
    explained = pca.explained_variance_ratio_
    print(f"PCA variance: PC1={explained[0]:.1%}, PC2={explained[1]:.1%}")

    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]

    colors = {"book": "#1f77b4", "music": "#ff7f0e"}
    markers = {"book": "o", "music": "^"}
    alphas = {"book": 0.35, "music": 0.45}

    fig, ax = plt.subplots(figsize=(12, 8))

    for itype, group in df.groupby("item_type"):
        ax.scatter(
            group["x"],
            group["y"],
            c=colors.get(itype, "grey"),
            marker=markers.get(itype, "s"),
            s=10,
            alpha=alphas.get(itype, 0.4),
            linewidths=0,
            label=f"{itype} (n={len(group)})",
        )

    # Centroids
    for itype, group in df.groupby("item_type"):
        cx, cy = group["x"].mean(), group["y"].mean()
        ax.scatter(
            cx,
            cy,
            c=colors.get(itype, "grey"),
            marker="*",
            s=300,
            edgecolors="black",
            linewidths=0.8,
            zorder=5,
        )
        ax.annotate(
            f"{itype} centroid",
            (cx, cy),
            textcoords="offset points",
            xytext=(8, 4),
            fontsize=9,
            color=colors.get(itype, "grey"),
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
