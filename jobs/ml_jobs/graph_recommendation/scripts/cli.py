"""Command line interface for building book metadata graphs."""

from __future__ import annotations

from pathlib import Path

import torch
import typer

from src.graph_recommendation.embedding_builder import train_metapath2vec
from src.graph_recommendation.graph_builder import (
    build_book_metadata_graph,
)
from src.graph_recommendation.heterograph_builder import build_book_metadata_heterograph

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

GRAPH_OUTPUT_OPTION = typer.Option(
    Path("data/book_metadata_graph.pt"),
    "--output",
    "-o",
    help="Where to save the serialized PyG Data object.",
    dir_okay=False,
)

EMBEDDING_OUTPUT_OPTION = typer.Option(
    Path("data/book_metadata_embeddings.parquet"),
    "--output-embeddings",
    "-e",
    help="Where to save the node embeddings as a parquet file.",
    dir_okay=False,
)

NROWS_OPTION = typer.Option(
    None,
    "--nrows",
    help="Optional number of rows to load from the parquet file.",
)

NUM_WORKERS_OPTION = typer.Option(
    8,
    "--num-workers",
    "-w",
    help="Number of worker processes to use.",
)


@app.command("build-graph")
def build_graph_command(
    parquet_path: Path = PARQUET_ARGUMENT,
    output_path: Path = GRAPH_OUTPUT_OPTION,
    nrows: int | None = NROWS_OPTION,
) -> None:
    """Build the book-to-metadata graph and save it to disk."""

    graph_data = build_book_metadata_graph(
        parquet_path,
        nrows=nrows,
    )
    torch.save(graph_data, output_path)

    typer.secho(
        (
            f"Graph saved to {output_path} "
            f"(nodes={graph_data.num_nodes}, "
            f"edges={graph_data.num_edges}, "
            f"books={len(graph_data.book_ids)}, "
            f"metadata={len(graph_data.metadata_ids)})"
        ),
        fg=typer.colors.GREEN,
    )


@app.command("build-heterograph")
def build_heterograph_command(
    parquet_path: Path = PARQUET_ARGUMENT,
    output_path: Path = GRAPH_OUTPUT_OPTION,
    nrows: int | None = NROWS_OPTION,
) -> None:
    """Build the book-to-metadata graph and save it to disk."""

    graph_data = build_book_metadata_heterograph(
        parquet_path,
        nrows=nrows,
    )
    torch.save(graph_data, output_path)

    typer.secho(
        (
            f"Graph saved to {output_path} "
            f"(nodes={graph_data.num_nodes}, "
            f"edges={graph_data.num_edges}, "
            f"books={len(graph_data.book_ids)}, "
            f"metadata={len(graph_data.metadata_ids)})"
        ),
        fg=typer.colors.GREEN,
    )


@app.command("train-metapath2vec")
def train_metapath2vec_command(
    parquet_path: Path = PARQUET_ARGUMENT,
    embedding_output_path: Path = EMBEDDING_OUTPUT_OPTION,
    num_workers: int = NUM_WORKERS_OPTION,
    nrows: int | None = NROWS_OPTION,
) -> None:
    """Train a Metapath2Vec model on the book-to-metadata graph and save it to disk."""

    graph_data = build_book_metadata_heterograph(
        parquet_path,
        nrows=nrows,
    )

    embeddings_df = train_metapath2vec(
        graph_data=graph_data,
        num_workers=num_workers,
    )

    embeddings_df.to_parquet(embedding_output_path, index=False)
    typer.secho(f"Embeddings saved to {embedding_output_path}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
