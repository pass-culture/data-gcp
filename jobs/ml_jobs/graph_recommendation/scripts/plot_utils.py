"""Shared utilities for graph recommendation plot scripts.

This module centralises constants, data-loading helpers, dimensionality-reduction
wrappers, and plotting utilities that would otherwise be duplicated across the
individual ``plot_*.py`` scripts.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

if TYPE_CHECKING:
    import argparse

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

ITEM_TYPE_MARKERS: dict[str, str] = {"book": "o", "music": "^"}
ITEM_TYPE_COLORS: dict[str, str] = {"book": "#1f77b4", "music": "#ff7f0e"}
DEFAULT_MARKER: str = "s"

# ---------------------------------------------------------------------------
# Path utilities
# ---------------------------------------------------------------------------


def safe_resolve_path(raw_path: str, *, must_exist: bool = True) -> Path:
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


# ---------------------------------------------------------------------------
# Parquet / data-loading helpers
# ---------------------------------------------------------------------------


def load_parquet_or_dir(raw_path: str) -> pd.DataFrame:
    """Load parquet data from a single file or a recursive directory tree.

    Args:
        raw_path: Path to a parquet file or a directory containing parquet files.

    Returns:
        Concatenated DataFrame of all parquet files found.

    Raises:
        ValueError: If the path contains illegal characters.
        FileNotFoundError: If *raw_path* is a directory with no parquet files.
    """
    safe_path = safe_resolve_path(raw_path)

    if safe_path.is_dir():
        files: list[Path] = sorted(
            f
            for f in safe_path.glob("**/*.parquet")
            if f.resolve().is_relative_to(safe_path)
        )
        if not files:
            raise FileNotFoundError(f"No .parquet files found under: {safe_path}")
        print(f"  Found {len(files)} parquet file(s) under {safe_path}")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    return pd.read_parquet(str(safe_path))


def load_embeddings(
    parquet_path: str,
    raw_data_path: str,
    *,
    include_gtl: bool = True,
    filter_gtl_level2: bool = False,
) -> pd.DataFrame:
    """Load embeddings and join item_type (and optionally GTL) from the raw dataset.

    Args:
        parquet_path:     Path to the embeddings parquet file
                          (columns: node_ids, embedding).
        raw_data_path:    Path to the raw parquet file or directory (columns:
                          item_id, item_type[, raw_gtl_id]).
        include_gtl:      When *True* (default), also join ``raw_gtl_id`` from
                          the raw data, dropping any existing ``gtl_id`` column
                          first, and renaming the joined column to ``gtl_id``.
        filter_gtl_level2: When *True*, keep only rows with a valid GTL level-2
                          code after loading (calls :func:`filter_gtl_level2_rows`).

    Returns:
        Merged DataFrame with columns: node_ids, embedding, item_id, item_type
        [, gtl_id].
    """
    cols = ["item_id", "item_type"]
    if include_gtl:
        cols.append("raw_gtl_id")

    raw = pd.read_parquet(raw_data_path).loc[:, cols]
    df = pd.read_parquet(parquet_path)

    if include_gtl:
        df = df.drop(columns=["gtl_id"], errors="ignore")

    df = df.merge(raw, left_on="node_ids", right_on="item_id", how="left")

    if include_gtl:
        df = df.rename(columns={"raw_gtl_id": "gtl_id"})

    print(f"Loaded {len(df)} embeddings")
    print(df["item_type"].value_counts().to_string())

    if filter_gtl_level2:
        df = filter_gtl_level2_rows(df)

    return df


def filter_gtl_level2_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with a valid GTL level-2 code.

    A valid GTL level-2 code ends in ``'0000'`` and is not ``'00000000'``.

    Args:
        df: DataFrame with a ``gtl_id`` column.

    Returns:
        Filtered copy of *df*.
    """
    return df[
        df["gtl_id"].notna()
        & df["gtl_id"].astype(str).str.endswith("0000")
        & (df["gtl_id"].astype(str) != "00000000")
    ].copy()


# ---------------------------------------------------------------------------
# Subsampling
# ---------------------------------------------------------------------------


def subsample_df(
    df: pd.DataFrame,
    nrows: int | None,
    *,
    random_state: int = 42,
) -> pd.DataFrame:
    """Return *df* subsampled to *nrows* rows when larger.

    Args:
        df:           Input DataFrame.
        nrows:        Maximum number of rows.  No-op when *None* or when
                      ``len(df) <= nrows``.
        random_state: Random seed for reproducible sampling.

    Returns:
        Original or sampled DataFrame.
    """
    if nrows and len(df) > nrows:
        df = df.sample(nrows, random_state=random_state)
        print(f"Subsampled to {len(df)} rows")
    return df


# ---------------------------------------------------------------------------
# Dimensionality reduction
# ---------------------------------------------------------------------------


def run_pca_2d(
    df: pd.DataFrame,
    *,
    embedding_col: str = "embedding",
    random_state: int = 42,
) -> tuple[np.ndarray, np.ndarray]:
    """Fit a 2-component PCA on the embedding column and return the projected coords.

    Args:
        df:            DataFrame that contains the embedding column.
        embedding_col: Name of the column holding embedding vectors.
        random_state:  Random seed for reproducibility.

    Returns:
        A tuple ``(coords, explained_variance_ratio)`` where *coords* has shape
        ``(n_items, 2)`` and *explained_variance_ratio* has shape ``(2,)``.
    """
    embeddings = np.stack(df[embedding_col].values)
    pca = PCA(n_components=2, random_state=random_state)
    coords = pca.fit_transform(embeddings)
    explained = pca.explained_variance_ratio_
    print(f"PCA variance: PC1={explained[0]:.1%}, PC2={explained[1]:.1%}")
    return coords, explained


# ---------------------------------------------------------------------------
# Centroid computation
# ---------------------------------------------------------------------------


def compute_centroids(df: pd.DataFrame) -> pd.DataFrame:
    """Compute one centroid embedding per (gtl_id, item_type) group.

    Args:
        df: DataFrame with columns: embedding, gtl_id, item_type.
            The DataFrame should already be filtered to the desired GTL level
            before calling this function.

    Returns:
        DataFrame with columns: cluster, gtl_id, item_type, n_items, centroid.
        Sorted by (item_type, gtl_id) with a reset index.
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

    result = (
        pd.DataFrame(records)
        .sort_values(["item_type", "gtl_id"])
        .reset_index(drop=True)
    )
    print(f"\nFound {len(result)} clusters (gtl_id x item_type)")
    return result


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------


def save_or_show(output_path: str | None, *, dpi: int = 150) -> None:
    """Save the current matplotlib figure or display it interactively.

    Args:
        output_path: Destination file path (PNG, PDF …).  When *None*, the
                     figure is displayed interactively via :func:`plt.show`.
        dpi:         Resolution used when saving to a raster format.
    """
    if output_path:
        plt.savefig(output_path, dpi=dpi, bbox_inches="tight")
        print(f"Saved to {output_path}")
    else:
        plt.show()


def build_gtl_legend_handles(
    gtl_values: list[str],
    gtl_color: dict,
    *,
    markersize: int = 9,
) -> list:
    """Build matplotlib legend handles for GTL color encoding.

    Args:
        gtl_values:  Ordered list of GTL codes to include in the legend.
        gtl_color:   Mapping from GTL code to a matplotlib color.
        markersize:  Size of the circular marker used in each handle.

    Returns:
        List of :class:`matplotlib.lines.Line2D` legend handles.
    """
    return [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=gtl_color[gtl],
            markersize=markersize,
            label=gtl,
        )
        for gtl in gtl_values
    ]


def build_item_type_legend_handles(
    item_types: list[str],
    *,
    markersize: int = 9,
) -> list:
    """Build matplotlib legend handles for item-type shape encoding.

    Args:
        item_types:  Ordered list of item type labels to include.
        markersize:  Size of the marker used in each handle.

    Returns:
        List of :class:`matplotlib.lines.Line2D` legend handles.
    """
    return [
        plt.Line2D(
            [0],
            [0],
            marker=ITEM_TYPE_MARKERS.get(item_type, DEFAULT_MARKER),
            color="grey",
            markersize=markersize,
            linestyle="None",
            label=item_type,
        )
        for item_type in item_types
    ]


# ---------------------------------------------------------------------------
# Argparse helpers
# ---------------------------------------------------------------------------


def add_embedding_args(parser: argparse.ArgumentParser) -> None:
    """Register a positional ``parquet_path`` and a ``--embeddings`` option."""
    parser.add_argument("parquet_path", nargs="?", default=None)
    parser.add_argument("--embeddings", "-e", default=None)


def add_raw_data_arg(
    parser: argparse.ArgumentParser,
    *,
    help: str = "Raw input parquet or directory to join item_type and raw_gtl_id.",
) -> None:
    """Register the ``--raw-data`` / ``-r`` argument."""
    parser.add_argument("--raw-data", "-r", required=True, help=help)


def add_output_arg(
    parser: argparse.ArgumentParser,
    *,
    help: str = "Save plot to this path (PNG/PDF). If omitted, display interactively.",
) -> None:
    """Register the ``--output`` / ``-o`` argument."""
    parser.add_argument("--output", "-o", default=None, help=help)


def add_nrows_arg(parser: argparse.ArgumentParser) -> None:
    """Register the ``--nrows`` argument."""
    parser.add_argument(
        "--nrows",
        type=int,
        default=None,
        help="Subsample N rows before plotting (faster for large files).",
    )


def resolve_parquet_path(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> str:
    """Return the embeddings path, preferring ``--embeddings`` over the positional arg.

    Args:
        args:   Parsed :class:`argparse.Namespace`.
        parser: The :class:`argparse.ArgumentParser` used for error reporting.

    Returns:
        The resolved embeddings path string.
    """
    parquet_path = args.embeddings or args.parquet_path
    if not parquet_path:
        parser.error(
            "Provide embeddings path as positional argument or via --embeddings"
        )
    return parquet_path
