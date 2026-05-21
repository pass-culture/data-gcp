"""
Visualize GTL×item_type cluster separation via multiple complementary plots.

Plots produced:
  1. Clustermap (hierarchical heatmap) — shows which clusters are close and far,
     with dendrograms revealing the natural grouping structure.
  2. t-SNE of centroids — 2D projection of the cluster centroids only, labeled,
     colored by item_type, with lines connecting same-GTL book/music pairs.
  3. Intra vs inter-cluster distance boxplot — shows the distribution of
     within-type distances vs cross-type same-GTL distances.

Usage:
    uv run python scripts/visualize_clusters.py EMBEDDINGS.parquet
    uv run python scripts/visualize_clusters.py EMBEDDINGS.parquet --output-dir results/
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.spatial.distance import squareform
from sklearn.manifold import TSNE
from sklearn.metrics.pairwise import cosine_distances

# ---------------------------------------------------------------------------
# Data loading & centroid computation
# ---------------------------------------------------------------------------


def load_embeddings(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    print(
        f"Loaded {len(df)} embeddings | item_types: {df['item_type'].value_counts().to_dict()}"
    )
    # Keep only level-2 GTL codes
    df = df[
        df["gtl_id"].notna()
        & df["gtl_id"].astype(str).str.endswith("0000")
        & (df["gtl_id"].astype(str) != "00000000")
    ].copy()
    return df


def compute_centroids(df: pd.DataFrame) -> pd.DataFrame:
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


def compute_dist_matrix(centroids_df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    matrix = np.stack(centroids_df["centroid"].values)
    dist = cosine_distances(matrix)
    labels = centroids_df["cluster"].tolist()
    return dist, labels


# ---------------------------------------------------------------------------
# Plot 1 — Clustermap (hierarchical heatmap with dendrograms)
# ---------------------------------------------------------------------------


def plot_clustermap(
    centroids_df: pd.DataFrame,
    dist_matrix: np.ndarray,
    labels: list[str],
    output_path: str | None,
) -> None:
    n = len(labels)
    short = [f"{c.split('__')[0]}\n({'b' if 'book' in c else 'm'})" for c in labels]

    # Row colours: blue = book, orange = music
    row_colors = pd.Series(
        ["#1f77b4" if "book" in c else "#ff7f0e" for c in labels],
        index=short,
    )

    dist_df = pd.DataFrame(dist_matrix, index=short, columns=short)

    from scipy.cluster.hierarchy import linkage as scipy_linkage

    # Convert square distance matrix to condensed form to avoid ClusterWarning
    condensed = squareform(dist_matrix, checks=False)
    row_linkage = scipy_linkage(condensed, method="average")

    g = sns.clustermap(
        dist_df,
        cmap="RdYlGn_r",
        figsize=(max(12, n * 0.55), max(10, n * 0.5)),
        row_colors=row_colors,
        col_colors=row_colors,
        linewidths=0.3,
        linecolor="white",
        vmin=0,
        cbar_kws={"label": "Cosine distance"},
        dendrogram_ratio=0.15,
        row_linkage=row_linkage,
        col_linkage=row_linkage,
    )
    g.ax_heatmap.set_xticklabels(g.ax_heatmap.get_xticklabels(), fontsize=7)
    g.ax_heatmap.set_yticklabels(g.ax_heatmap.get_yticklabels(), fontsize=7)

    # Colour tick labels
    reordered = [t.get_text() for t in g.ax_heatmap.get_xticklabels()]
    for tick, label in zip(g.ax_heatmap.get_xticklabels(), reordered, strict=False):
        tick.set_color("#1f77b4" if "\nb" in label else "#ff7f0e")
    for tick, label in zip(g.ax_heatmap.get_yticklabels(), reordered, strict=False):
        tick.set_color("#1f77b4" if "\nb" in label else "#ff7f0e")

    # Legend

    book_p = mpatches.Patch(color="#1f77b4", label="book")
    music_p = mpatches.Patch(color="#ff7f0e", label="music")
    g.ax_heatmap.legend(
        handles=[book_p, music_p],
        loc="upper left",
        bbox_to_anchor=(1.25, 1.1),
        fontsize=8,
    )
    g.fig.suptitle(
        "Hierarchical clustermap of GTL×item_type centroids\n"
        "(green = close  |  red = far  |  color = item type)",
        y=1.01,
        fontsize=11,
    )

    _save_or_show(g.fig, output_path, "clustermap")


# ---------------------------------------------------------------------------
# Plot 2 — t-SNE of centroids with same-GTL connector lines
# ---------------------------------------------------------------------------


def plot_tsne_centroids(centroids_df: pd.DataFrame, output_path: str | None) -> None:
    n = len(centroids_df)
    centroid_matrix = np.stack(centroids_df["centroid"].values)

    perplexity = min(5, n - 1)
    tsne = TSNE(
        n_components=2,
        perplexity=perplexity,
        random_state=42,
        max_iter=2000,
        metric="cosine",
    )
    coords = tsne.fit_transform(centroid_matrix)

    centroids_df = centroids_df.copy()
    centroids_df["x"] = coords[:, 0]
    centroids_df["y"] = coords[:, 1]

    fig, ax = plt.subplots(figsize=(13, 9))

    colors = {"book": "#1f77b4", "music": "#ff7f0e"}
    markers = {"book": "o", "music": "^"}

    # Draw lines between same-GTL book/music pairs
    gtl_ids = centroids_df["gtl_id"].unique()
    for gtl in gtl_ids:
        pair = centroids_df[centroids_df["gtl_id"] == gtl]
        if len(pair) == 2:
            x_vals = pair["x"].values
            y_vals = pair["y"].values
            ax.plot(
                x_vals,
                y_vals,
                color="grey",
                linewidth=0.8,
                linestyle="--",
                alpha=0.5,
                zorder=1,
            )

    # Scatter per item_type
    for itype, group in centroids_df.groupby("item_type"):
        ax.scatter(
            group["x"],
            group["y"],
            c=colors.get(itype, "green"),
            marker=markers.get(itype, "s"),
            s=120,
            zorder=3,
            label=itype,
            edgecolors="white",
            linewidths=0.5,
        )

    # Label each point
    for _, row in centroids_df.iterrows():
        ax.annotate(
            f"{row['gtl_id']}\n({'b' if row['item_type'] == 'book' else 'm'})",
            (row["x"], row["y"]),
            textcoords="offset points",
            xytext=(5, 5),
            fontsize=6.5,
            color=colors.get(row["item_type"], "black"),
        )

    ax.legend(title="Item type", fontsize=9)
    ax.set_title(
        "t-SNE of GTL×item_type centroids\n"
        "(dashed lines connect same-GTL book/music pairs — longer line = better separation)",
        fontsize=11,
    )
    ax.set_xlabel("t-SNE 1")
    ax.set_ylabel("t-SNE 2")
    plt.tight_layout()
    _save_or_show(fig, output_path, "tsne_centroids")


# ---------------------------------------------------------------------------
# Plot 3 — Intra vs inter distance boxplot
# ---------------------------------------------------------------------------


def plot_distance_boxplot(
    centroids_df: pd.DataFrame,
    dist_matrix: np.ndarray,
    labels: list[str],
    output_path: str | None,
) -> None:
    rows = []
    n = len(labels)
    for i in range(n):
        for j in range(i + 1, n):
            src = centroids_df.iloc[i]
            dst = centroids_df.iloc[j]
            same_gtl = src["gtl_id"] == dst["gtl_id"]
            same_type = src["item_type"] == dst["item_type"]

            if same_gtl and not same_type:
                category = "Same GTL\ncross-type\n(book↔music)"
            elif same_type and src["item_type"] == "book":
                category = "Different GTL\nbook↔book"
            elif same_type and src["item_type"] == "music":
                category = "Different GTL\nmusic↔music"
            else:
                category = "Different GTL\nbook↔music"

            rows.append({"category": category, "cosine_distance": dist_matrix[i, j]})

    dist_df = pd.DataFrame(rows)

    order = [
        "Same GTL\ncross-type\n(book↔music)",
        "Different GTL\nbook↔music",
        "Different GTL\nbook↔book",
        "Different GTL\nmusic↔music",
    ]
    order = [o for o in order if o in dist_df["category"].unique()]

    palette = {
        "Same GTL\ncross-type\n(book↔music)": "#d62728",
        "Different GTL\nbook↔music": "#9467bd",
        "Different GTL\nbook↔book": "#1f77b4",
        "Different GTL\nmusic↔music": "#ff7f0e",
    }

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.boxplot(
        data=dist_df,
        x="category",
        y="cosine_distance",
        order=order,
        palette=palette,
        ax=ax,
        width=0.5,
        flierprops={"marker": ".", "markersize": 4},
    )
    sns.stripplot(
        data=dist_df,
        x="category",
        y="cosine_distance",
        order=order,
        color="black",
        alpha=0.4,
        size=5,
        ax=ax,
        jitter=True,
    )

    ax.set_title(
        "Cosine distance distribution between cluster centroids\n"
        "Key: same-GTL cross-type (red) should be HIGHER than same-type different-GTL",
        fontsize=11,
    )
    ax.set_xlabel("")
    ax.set_ylabel("Cosine distance")
    ax.axhline(
        dist_df[dist_df["category"] == "Same GTL\ncross-type\n(book↔music)"][
            "cosine_distance"
        ].min(),
        color="red",
        linestyle=":",
        linewidth=1,
        alpha=0.6,
        label="min same-GTL cross-type",
    )
    ax.legend(fontsize=8)
    plt.tight_layout()
    _save_or_show(fig, output_path, "distance_boxplot")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _save_or_show(fig, path: str | None, name: str) -> None:
    if path:
        fig.savefig(path, dpi=150, bbox_inches="tight")
        print(f"Saved: {path}")
    else:
        plt.show()
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Visualize GTL×item_type cluster separation."
    )
    parser.add_argument("parquet_path", help="Path to the embeddings parquet file.")
    parser.add_argument(
        "--output-dir",
        "-o",
        default=None,
        help="Directory to save plots. If omitted, display interactively.",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir) if args.output_dir else None
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    def out(name: str) -> str | None:
        return str(out_dir / name) if out_dir else None

    df = load_embeddings(args.parquet_path)
    centroids_df = compute_centroids(df)
    dist_matrix, labels = compute_dist_matrix(centroids_df)

    print("Generating clustermap…")
    plot_clustermap(centroids_df, dist_matrix, labels, out("clustermap.png"))

    print("Generating t-SNE of centroids…")
    plot_tsne_centroids(centroids_df, out("tsne_centroids.png"))

    print("Generating distance boxplot…")
    plot_distance_boxplot(
        centroids_df, dist_matrix, labels, out("distance_boxplot.png")
    )

    print("Done.")


if __name__ == "__main__":
    main()
