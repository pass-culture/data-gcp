"""
t-SNE projection of GTL x item_type cluster centroids.

For each (gtl_id, item_type) group, a centroid embedding is computed.
The centroids are then projected to 2D via t-SNE (cosine metric).
Dashed grey lines connect book/music pairs that share the same GTL code:
a longer line indicates better separation between the two item types.

Usage:
    uv run python scripts/plot_tsne_gtl_centroids.py results/embeddings.parquet \\
        --raw-data   data/raw_input/book_music_catalog \\
        --output     results/tsne_centroids.png
"""

from __future__ import annotations

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.manifold import TSNE

ITEM_TYPE_COLORS = {"book": "#1f77b4", "music": "#ff7f0e"}
ITEM_TYPE_MARKERS = {"book": "o", "music": "^"}


def load_embeddings(parquet_path: str, raw_data_path: str) -> pd.DataFrame:
    """Load embeddings and join item_type and GTL code from the raw dataset.

    Only GTL level-2 codes (ending in '0000', excluding '00000000') are kept.

    Args:
        parquet_path:   Path to the embeddings parquet file.
        raw_data_path:  Path to the raw parquet file or directory.

    Returns:
        Filtered DataFrame with columns:
        node_ids, embedding, item_id, item_type, gtl_id.
    """
    raw = pd.read_parquet(raw_data_path).loc[:, ["item_id", "item_type", "raw_gtl_id"]]
    df = (
        pd.read_parquet(parquet_path)
        .drop(columns=["gtl_id"], errors="ignore")
        .merge(raw, left_on="node_ids", right_on="item_id", how="left")
        .rename(columns={"raw_gtl_id": "gtl_id"})
    )

    print(
        f"Loaded {len(df)} embeddings"
        f" | item_types: {df['item_type'].value_counts().to_dict()}"
    )

    df = df[
        df["gtl_id"].notna()
        & df["gtl_id"].astype(str).str.endswith("0000")
        & (df["gtl_id"].astype(str) != "00000000")
    ].copy()

    return df


def compute_centroids(df: pd.DataFrame) -> pd.DataFrame:
    """Compute one centroid embedding per (gtl_id, item_type) group.

    Args:
        df: Filtered DataFrame from load_embeddings().

    Returns:
        DataFrame with columns: cluster, gtl_id, item_type, n_items, centroid.
        Sorted by (item_type, gtl_id) and index reset.
    """
    embeddings = np.stack(df["embedding"].values)

    records = []
    for (gtl_id, item_type), group in df.groupby(["gtl_id", "item_type"]):
        positions = [df.index.get_loc(i) for i in group.index]
        centroid = embeddings[positions].mean(axis=0)
        records.append(
            {
                "cluster": f"{gtl_id}__{item_type}",
                "gtl_id": gtl_id,
                "item_type": item_type,
                "n_items": len(group),
                "centroid": centroid,
            }
        )

    return (
        pd.DataFrame(records)
        .sort_values(["item_type", "gtl_id"])
        .reset_index(drop=True)
    )


def plot_tsne_centroids(centroids_df: pd.DataFrame, output_path: str | None) -> None:
    """Project cluster centroids to 2D with t-SNE and render the scatter plot.

    Dashed grey lines connect book/music pairs that share the same GTL code.
    A longer line indicates better separation between the two item types.

    Args:
        centroids_df: Output of compute_centroids().
        output_path:  Save path (PNG). Displays interactively when None.
    """
    n = len(centroids_df)
    centroid_matrix = np.stack(centroids_df["centroid"].values)

    tsne = TSNE(
        n_components=2,
        perplexity=min(5, n - 1),
        random_state=42,
        max_iter=2000,
        metric="cosine",
    )
    coords = tsne.fit_transform(centroid_matrix)

    centroids_df = centroids_df.copy()
    centroids_df["x"] = coords[:, 0]
    centroids_df["y"] = coords[:, 1]

    fig, ax = plt.subplots(figsize=(13, 9))

    # Dashed lines connecting same-GTL book/music pairs
    for gtl_id in centroids_df["gtl_id"].unique():
        pair = centroids_df[centroids_df["gtl_id"] == gtl_id]
        if len(pair) == 2:
            ax.plot(
                pair["x"].values,
                pair["y"].values,
                color="grey",
                linewidth=0.8,
                linestyle="--",
                alpha=0.5,
                zorder=1,
            )

    # Scatter one layer per item type
    for item_type, group in centroids_df.groupby("item_type"):
        ax.scatter(
            group["x"],
            group["y"],
            c=ITEM_TYPE_COLORS.get(item_type, "green"),
            marker=ITEM_TYPE_MARKERS.get(item_type, "s"),
            s=120,
            zorder=3,
            label=item_type,
            edgecolors="white",
            linewidths=0.5,
        )

    # Label each centroid point
    for _, row in centroids_df.iterrows():
        ax.annotate(
            f"{row['gtl_id']}\n({'b' if row['item_type'] == 'book' else 'm'})",
            (row["x"], row["y"]),
            textcoords="offset points",
            xytext=(5, 5),
            fontsize=6.5,
            color=ITEM_TYPE_COLORS.get(row["item_type"], "black"),
        )

    ax.legend(title="Item type", fontsize=9)
    ax.set_title(
        "t-SNE of GTL x item_type centroids\n"
        "(dashed lines connect same-GTL book/music pairs"
        " — longer line = better separation)",
        fontsize=11,
    )
    ax.set_xlabel("t-SNE 1")
    ax.set_ylabel("t-SNE 2")
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
    else:
        plt.show()
    plt.close(fig)


def main() -> None:
    """Parse CLI arguments and render the t-SNE centroid plot."""
    parser = argparse.ArgumentParser(
        description="t-SNE projection of GTL x item_type cluster centroids."
    )
    parser.add_argument("parquet_path", help="Path to the embeddings parquet file.")
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
        help="Save plot to this path (PNG). If omitted, display interactively.",
    )
    args = parser.parse_args()

    df = load_embeddings(args.parquet_path, args.raw_data)
    centroids_df = compute_centroids(df)

    print("Generating t-SNE of centroids…")
    plot_tsne_centroids(centroids_df, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
