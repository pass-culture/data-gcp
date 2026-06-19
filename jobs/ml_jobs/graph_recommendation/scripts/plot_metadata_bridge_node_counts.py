"""
Visualize metadata nodes that bridge books AND music in the graph.

A "bridge node" is a metadata value (artist_id, gtl_label_level_1,
gtl_label_level_2, series_id, music_label, distributor…) that is connected
to at least one book AND at least one music item.

For each metadata column, the script produces:
  - A count of book-only, music-only, and bridge nodes
  - A two-panel summary bar chart across all metadata columns
  - For artist_id: a horizontal bar chart of the top bridge artists
  - For GTL columns: a grouped bar chart of bridge nodes

Usage:
    uv run python scripts/plot_metadata_bridge_node_counts.py \\
        --raw-data   data/raw_input/input_with_prefix.parquet \\
        --output-dir results/bridge_nodes/
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

METADATA_COLUMNS = [
    "artist_id",
    "gtl_label_level_1",
    "gtl_label_level_2",
    "series_id",
    "music_label",
    "distributor",
]

COLOR_BOOK = "#1565C0"
COLOR_MUSIC = "#E53935"
COLOR_BRIDGE = "#2E7D32"


def _safe_resolve_path(raw_path: str, *, must_exist: bool = True) -> Path:
    """Resolve and validate a user-supplied path against path injection attacks.

    Resolves the path to its canonical absolute form (expanding ``..`` and
    symlinks) and rejects inputs that contain null bytes.

    Args:
        raw_path:   Raw path string supplied via CLI or an external source.
        must_exist: When *True* (default), raise :exc:`FileNotFoundError` if
                    the resolved path does not exist on disk.  Set to *False*
                    for output paths that are about to be created.

    Returns:
        Resolved :class:`~pathlib.Path` object.

    Raises:
        ValueError: If the path contains null bytes or other illegal characters.
        FileNotFoundError: If *must_exist* is True and the resolved path does
            not exist.
    """
    if "\x00" in raw_path:
        raise ValueError("Path contains null bytes, which is not allowed.")

    resolved = Path(raw_path).resolve()

    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {resolved}")

    return resolved


def load_raw(raw_path: str) -> pd.DataFrame:
    """Load the raw item dataset from a parquet file or a directory tree.

    Args:
        raw_path: Path to a parquet file or a directory containing parquet files.

    Returns:
        Concatenated DataFrame of all parquet files found.

    Raises:
        ValueError: If the path is invalid or contains illegal characters.
        FileNotFoundError: If raw_path is a directory with no parquet files.
    """
    safe_path = _safe_resolve_path(raw_path)

    if safe_path.is_dir():
        files = sorted(safe_path.glob("**/*.parquet"))
        if not files:
            raise FileNotFoundError(f"No .parquet files found under: {safe_path}")
        # Ensure every discovered file is still a descendant of safe_path
        files = [f for f in files if f.resolve().is_relative_to(safe_path)]
        print(f"  Found {len(files)} parquet file(s) under {safe_path}")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return pd.read_parquet(safe_path)


def classify_nodes(
    df: pd.DataFrame, col: str, label_col: str | None = None
) -> pd.DataFrame:
    """For each unique value of *col*, count connected book and music items.

    Each node is classified as:
      - "book only"            → connected to books but not music
      - "music only"           → connected to music but not books
      - "bridge (book + music)" → connected to both

    Args:
        df:        Raw item DataFrame with at least 'item_type' and *col* columns.
        col:       Metadata column to analyze (e.g. 'artist_id').
        label_col: Optional human-readable label column to attach (e.g. 'artist_name').

    Returns:
        DataFrame with columns: col, book, music, [label_col], is_bridge, category.
    """
    extra_cols = [label_col] if label_col and label_col in df.columns else []
    sub = df[df[col].notna()][["item_type", col, *extra_cols]].copy()
    sub[col] = sub[col].astype(str)

    counts = sub.groupby([col, "item_type"]).size().unstack(fill_value=0).reset_index()
    counts.columns.name = None

    for item_type in ["book", "music"]:
        if item_type not in counts.columns:
            counts[item_type] = 0

    if label_col and label_col in df.columns:
        label_map = (
            df[df[col].notna()][[col, label_col]]
            .drop_duplicates(subset=[col])
            .set_index(col)[label_col]
            .astype(str)
        )
        counts[label_col] = counts[col].map(label_map).fillna(counts[col])
    else:
        counts[label_col if label_col else col] = counts[col]

    counts["is_bridge"] = (counts["book"] > 0) & (counts["music"] > 0)
    counts["category"] = counts["is_bridge"].map(
        {True: "bridge (book + music)", False: None}
    )
    counts.loc[~counts["is_bridge"] & (counts["book"] > 0), "category"] = "book only"
    counts.loc[~counts["is_bridge"] & (counts["music"] > 0), "category"] = "music only"

    return counts


def plot_summary(stats: dict[str, dict], output_path: str | None) -> None:
    """Two-panel bar chart summarizing bridge node counts across all metadata columns.

    Left panel  — 100% stacked bar (proportion of bridge vs single-type nodes).
    Right panel — Absolute counts with bridge count annotated on top.

    Args:
        stats:       Dict mapping each metadata column to its category counts.
        output_path: Save path (PNG). Displays interactively when None.
    """
    cols = list(stats.keys())
    book_only = [stats[c]["book only"] for c in cols]
    music_only = [stats[c]["music only"] for c in cols]
    bridge = [stats[c]["bridge (book + music)"] for c in cols]
    totals = [
        b + m + br for b, m, br in zip(book_only, music_only, bridge, strict=False)
    ]

    book_pct = [b / max(t, 1) * 100 for b, t in zip(book_only, totals, strict=False)]
    music_pct = [m / max(t, 1) * 100 for m, t in zip(music_only, totals, strict=False)]
    bridge_pct = [br / max(t, 1) * 100 for br, t in zip(bridge, totals, strict=False)]

    x = range(len(cols))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # --- Left panel: 100% stacked bar (proportions) ---
    ax1.bar(list(x), book_pct, color=COLOR_BOOK, label="book only")
    ax1.bar(list(x), music_pct, color=COLOR_MUSIC, label="music only", bottom=book_pct)
    ax1.bar(
        list(x),
        bridge_pct,
        color=COLOR_BRIDGE,
        label="bridge (book+music)",
        bottom=[b + m for b, m in zip(book_pct, music_pct, strict=False)],
    )

    for i, pct in enumerate(bridge_pct):
        if pct > 0:
            ax1.text(
                i,
                book_pct[i] + music_pct[i] + pct / 2,
                f"{pct:.1f}%",
                ha="center",
                va="center",
                fontsize=8,
                color="white",
                fontweight="bold",
            )

    ax1.set_xticks(list(x))
    ax1.set_xticklabels(cols, rotation=25, ha="right")
    ax1.set_ylabel("% of unique metadata nodes")
    ax1.set_ylim(0, 100)
    ax1.set_title(
        "Proportion of bridge nodes\n(connected to both book & music)", fontsize=11
    )
    ax1.legend(loc="upper right", fontsize=8)

    # --- Right panel: absolute counts ---
    bar_width = 0.5
    ax2.bar(
        list(x), book_only, bar_width, color=COLOR_BOOK, alpha=0.4, label="book only"
    )
    ax2.bar(
        list(x),
        music_only,
        bar_width,
        color=COLOR_MUSIC,
        alpha=0.4,
        label="music only",
        bottom=book_only,
    )
    ax2.bar(
        list(x),
        bridge,
        bar_width,
        color=COLOR_BRIDGE,
        label="bridge (book+music)",
        bottom=[b + m for b, m in zip(book_only, music_only, strict=False)],
    )

    for i, bridge_count in enumerate(bridge):
        ax2.text(
            i,
            totals[i] + max(totals) * 0.01,
            f"{bridge_count}",
            ha="center",
            va="bottom",
            fontsize=9,
            color=COLOR_BRIDGE,
            fontweight="bold",
        )

    ax2.set_xticks(list(x))
    ax2.set_xticklabels(cols, rotation=25, ha="right")
    ax2.set_ylabel("Number of unique metadata nodes")
    ax2.set_title("Absolute counts\n(green number = bridge count)", fontsize=11)
    ax2.legend(loc="upper right", fontsize=8)

    plt.suptitle(
        "Metadata nodes: book-only vs music-only vs bridge", fontsize=13, y=1.01
    )
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
    else:
        plt.show()
    plt.close()


def plot_top_bridge_artists(
    counts: pd.DataFrame, output_path: str | None, top_n: int = 30
) -> None:
    """Horizontal bar chart of top bridge artists ranked by total connected items.

    Args:
        counts:      Output of classify_nodes() for the 'artist_id' column.
        output_path: Save path (PNG). Displays interactively when None.
        top_n:       Maximum number of artists to display.
    """
    bridge_artists = counts[counts["is_bridge"]].copy()
    bridge_artists["total"] = bridge_artists["book"] + bridge_artists["music"]
    bridge_artists = bridge_artists.nlargest(top_n, "total")

    label_col = (
        "artist_name" if "artist_name" in bridge_artists.columns else "artist_id"
    )
    labels = bridge_artists[label_col].values

    fig, ax = plt.subplots(figsize=(10, max(6, top_n * 0.35)))

    y = range(len(bridge_artists))
    ax.barh(list(y), bridge_artists["book"].values, color=COLOR_BOOK, label="books")
    ax.barh(
        list(y),
        bridge_artists["music"].values,
        color=COLOR_MUSIC,
        label="music items",
        left=bridge_artists["book"].values,
    )

    ax.set_yticks(list(y))
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Number of connected items")
    ax.set_title(
        f"Top {top_n} bridge artists (connected to both books and music)", fontsize=11
    )
    ax.legend()
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
    else:
        plt.show()
    plt.close()


def plot_bridge_gtl(counts: pd.DataFrame, col: str, output_path: str | None) -> None:
    """Grouped bar chart showing book vs music item counts for each bridge GTL node.

    Args:
        counts:      Output of classify_nodes() for a GTL column.
        col:         Name of the GTL column (used for axis labels and title).
        output_path: Save path (PNG). Displays interactively when None.
    """
    bridge_nodes = counts[counts["is_bridge"]].copy()
    bridge_nodes["total"] = bridge_nodes["book"] + bridge_nodes["music"]
    bridge_nodes = bridge_nodes.sort_values("total", ascending=False)

    width = 0.4
    x = range(len(bridge_nodes))

    fig, ax = plt.subplots(figsize=(12, max(5, len(bridge_nodes) * 0.4)))

    ax.bar(
        [i - width / 2 for i in x],
        bridge_nodes["book"].values,
        width,
        color=COLOR_BOOK,
        label="books",
    )
    ax.bar(
        [i + width / 2 for i in x],
        bridge_nodes["music"].values,
        width,
        color=COLOR_MUSIC,
        label="music",
    )

    ax.set_xticks(list(x))
    ax.set_xticklabels(bridge_nodes[col].values, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Number of connected items")
    ax.set_title(
        f"Bridge nodes in '{col}' (connected to both books and music)", fontsize=11
    )
    ax.legend()
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved: {output_path}")
    else:
        plt.show()
    plt.close()


def main() -> None:
    """Parse CLI arguments and generate all bridge node visualizations."""
    parser = argparse.ArgumentParser(
        description="Visualize metadata nodes that bridge books and music."
    )
    parser.add_argument(
        "--raw-data", "-r", required=True, help="Raw input parquet or directory."
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default=None,
        help="Directory to save plots. If omitted, display interactively.",
    )
    args = parser.parse_args()

    out_dir = (
        _safe_resolve_path(args.output_dir, must_exist=False)
        if args.output_dir
        else None
    )
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)

    def output_path(filename: str) -> str | None:
        return str(out_dir / filename) if out_dir else None

    df = load_raw(args.raw_data)
    print(
        f"Loaded {len(df)} rows"
        f" | item_types: {df['item_type'].value_counts().to_dict()}"
    )

    stats: dict[str, dict] = {}
    all_counts: dict[str, pd.DataFrame] = {}

    for col in METADATA_COLUMNS:
        if col not in df.columns:
            print(f"  Skipping '{col}' (not in data)")
            continue

        label_col = "artist_name" if col == "artist_id" else None
        counts = classify_nodes(df, col, label_col=label_col)
        all_counts[col] = counts

        cat_counts = counts["category"].value_counts().to_dict()
        stats[col] = {
            "book only": cat_counts.get("book only", 0),
            "music only": cat_counts.get("music only", 0),
            "bridge (book + music)": cat_counts.get("bridge (book + music)", 0),
        }

        bridge_count = stats[col]["bridge (book + music)"]
        total_count = sum(stats[col].values())
        print(
            f"  {col:25s} → {total_count:5d} nodes total | "
            f"{bridge_count:4d} bridge ({bridge_count / max(total_count, 1):.0%})"
        )

    print("\nGenerating summary chart…")
    plot_summary(stats, output_path("bridge_summary.png"))

    if "artist_id" in all_counts:
        print("Generating top bridge artists chart…")
        plot_top_bridge_artists(
            all_counts["artist_id"], output_path("bridge_top_artists.png")
        )

    for col in ["gtl_label_level_1", "gtl_label_level_2"]:
        if col in all_counts and all_counts[col]["is_bridge"].any():
            print(f"Generating bridge chart for {col}…")
            plot_bridge_gtl(all_counts[col], col, output_path(f"bridge_{col}.png"))

    print("Done.")


if __name__ == "__main__":
    main()
