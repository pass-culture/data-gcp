"""Command line interface for building book metadata graphs."""

from __future__ import annotations

from pathlib import Path

import typer

from src.graph_recommendation.graph_builder import (
    DEFAULT_METADATA_COLUMNS,
    build_book_metadata_graph,
)

APP_DESCRIPTION = (
    "Utilities to build PyTorch Geometric graphs for book recommendations."
)
app = typer.Typer(help=APP_DESCRIPTION)

PARQUET_ARGUMENT = typer.Argument(
    ...,
    exists=True,
    readable=True,
    help="Input parquet file.",
)

OUTPUT_OPTION = typer.Option(
    Path("book_metadata_graph.pt"),
    "--output",
    "-o",
    help="Where to save the serialized PyG Data object.",
    dir_okay=False,
)

METADATA_OPTION = typer.Option(
    None,
    "--metadata-column",
    "-m",
    help=(
        "Column name to include as metadata. Repeat for multiple. "
        "Defaults to rayon and the GTL label levels."
    ),
    multiple=True,
)

NROWS_OPTION = typer.Option(
    None,
    "--nrows",
    help="Optional number of rows to load from the parquet file.",
)

SUMMARY_NROWS_OPTION = typer.Option(
    None,
    "--nrows",
    help="Optional subset of rows to inspect.",
)


def _resolve_metadata_columns(columns: tuple[str, ...] | None) -> list[str]:
    if columns:
        return list(columns)
    return list(DEFAULT_METADATA_COLUMNS)


@app.command("build-graph")
def build_graph_command(
    parquet_path: Path = PARQUET_ARGUMENT,
    output_path: Path = OUTPUT_OPTION,
    metadata_column: tuple[str, ...] | None = METADATA_OPTION,
    nrows: int | None = NROWS_OPTION,
) -> None:
    """Build the book-to-metadata graph and save it to disk."""

    metadata_columns = _resolve_metadata_columns(metadata_column)
    graph = build_book_metadata_graph(
        parquet_path,
        metadata_columns=metadata_columns,
        nrows=nrows,
    )
    graph.save(output_path)

    typer.secho(
        (
            f"Graph saved to {output_path} "
            f"(nodes={graph.data.num_nodes}, edges={graph.data.num_edges}, "
            f"books={len(graph.book_ids)}, metadata={len(graph.metadata_keys)})"
        ),
        fg=typer.colors.GREEN,
    )


@app.command("summary")
def summarize_command(
    parquet_path: Path = PARQUET_ARGUMENT,
    metadata_column: tuple[str, ...] | None = METADATA_OPTION,
    nrows: int | None = SUMMARY_NROWS_OPTION,
) -> None:
    """Print a quick summary of the graph that would be created."""

    metadata_columns = _resolve_metadata_columns(metadata_column)
    graph = build_book_metadata_graph(
        parquet_path,
        metadata_columns=metadata_columns,
        nrows=nrows,
    )
    typer.secho(
        (
            "Graph summary -> "
            f"nodes={graph.data.num_nodes} (books={len(graph.book_ids)}, "
            f"metadata={len(graph.metadata_keys)}), "
            f"edges={graph.data.num_edges}, "
            f"metadata columns={', '.join(metadata_columns)}"
        ),
        fg=typer.colors.BLUE,
    )


if __name__ == "__main__":
    app()
