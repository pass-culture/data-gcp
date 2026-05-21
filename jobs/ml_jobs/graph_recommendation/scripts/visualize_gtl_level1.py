"""PCA 2D — colored by item_type × GTL level-1, shaped by item_type.

Same GTL level-1 + same item_type → same color.
Same GTL level-1 but different item_type → same hue family but different shape (○/▲).
Different GTL level-1 → different color regardless of item_type.
"""

from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA


def gtl_level1(gtl_id: str) -> str:
    """Extract GTL level-1 code: first 2 digits + '000000'."""
    s = str(gtl_id).strip()
    if len(s) >= 2 and s not in ("nan", "None", "00000000"):
        return s[:2] + "000000"
    return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PCA 2D colored by item_type × GTL level-1."
    )
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

    # Compute PCA
    embeddings = np.stack(df["embedding"].values)
    pca = PCA(n_components=2, random_state=42)
    coords = pca.fit_transform(embeddings)
    explained = pca.explained_variance_ratio_
    print(f"PCA variance: PC1={explained[0]:.1%}, PC2={explained[1]:.1%}")

    df = df.copy()
    df["x"] = coords[:, 0]
    df["y"] = coords[:, 1]
    df["gtl_l1"] = df["gtl_id"].apply(gtl_level1)

    # Build color palette: one color per GTL level-1 code
    gtl_l1_values = sorted(v for v in df["gtl_l1"].unique() if v != "unknown")
    n_gtl = len(gtl_l1_values)
    # Use tab20 for ≤20 groups, hsv otherwise
    cmap = plt.colormaps["tab20" if n_gtl <= 20 else "hsv"].resampled(max(n_gtl, 1))
    gtl_color = {g: cmap(i) for i, g in enumerate(gtl_l1_values)}
    gtl_color["unknown"] = (0.7, 0.7, 0.7, 0.3)

    markers = {"book": "o", "music": "^"}
    default_marker = "s"
    item_types = sorted(df["item_type"].unique())

    fig, ax = plt.subplots(figsize=(14, 10))

    for gtl in gtl_l1_values:
        for itype in item_types:
            mask = (df["gtl_l1"] == gtl) & (df["item_type"] == itype)
            subset = df[mask]
            if subset.empty:
                continue
            ax.scatter(
                subset["x"],
                subset["y"],
                c=[gtl_color[gtl]],
                marker=markers.get(itype, default_marker),
                s=14,
                alpha=0.55,
                linewidths=0,
            )

    # --- Legends ---
    # GTL level-1 colors
    handles_gtl = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=gtl_color[g],
            markersize=9,
            label=g,
        )
        for g in gtl_l1_values
    ]
    # item_type markers
    handles_type = [
        plt.Line2D(
            [0],
            [0],
            marker=markers.get(t, default_marker),
            color="grey",
            markersize=9,
            linestyle="None",
            label=t,
        )
        for t in item_types
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
