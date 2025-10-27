"""Command line interface for building book metadata graphs."""

from __future__ import annotations

import json

import torch
import typer

from src.constants import RESULTS_DIR
from src.embedding_builder import train_metapath2vec
from src.evaluation import DEFAULT_EVAL_CONFIG, evaluate_embeddings
from src.graph_builder import (
    build_book_metadata_graph,
)
from src.heterograph_builder import build_book_metadata_heterograph

APP_DESCRIPTION = (
    "Utilities to build PyTorch Geometric graphs for book recommendations."
)
app = typer.Typer(help=APP_DESCRIPTION)

PARQUET_ARGUMENT = typer.Argument(
    ...,
    help="Input parquet file. Can be a local path or a GCS path (gs://...).",
)

GRAPH_OUTPUT_OPTION = typer.Option(
    f"{RESULTS_DIR}/book_metadata_graph.pt",
    "--output",
    "-o",
    help="Where to save the serialized PyG Data object.",
    dir_okay=False,
)

EMBEDDING_OUTPUT_OPTION = typer.Option(
    f"{RESULTS_DIR}/book_metadata_embeddings.parquet",
    "--output-embeddings",
    "-e",
    help="Where to save the node embeddings as a parquet file. "
    "Can be a local path or a GCS path (gs://...).",
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


RAW_DATA_PATH_ARGUMENT = typer.Argument(
    ...,
    help="Path to raw metadata parquet (can be glob pattern like data-*.parquet).",
)

EMBEDDING_PATH_ARGUMENT = typer.Argument(
    ...,
    help="Path to embeddings parquet file.",
)

METRICS_OUTPUT_ARGUMENT = typer.Argument(
    ...,
    help="Where to save the evaluation metrics CSV file.",
    dir_okay=False,
)

PAIRWISE_SCORE_OUTPUT_OPTION = typer.Option(
    None,
    "--output-scores-path",
    help="Where to save the pairwise scores parquet file.",
    dir_okay=False,
)

EVAL_CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    help="Path to JSON config file to override default evaluation parameters.",
)


@app.command("build-graph")
def build_graph_command(
    parquet_path: str = PARQUET_ARGUMENT,
    output_path: str = GRAPH_OUTPUT_OPTION,
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
    parquet_path: str = PARQUET_ARGUMENT,
    output_path: str = GRAPH_OUTPUT_OPTION,
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
    parquet_path: str = PARQUET_ARGUMENT,
    embedding_output_path: str = EMBEDDING_OUTPUT_OPTION,
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


@app.command("evaluate-metapath2vec")
def evaluate_metapath2vec_command(
    raw_data_path: str = RAW_DATA_PATH_ARGUMENT,
    embedding_path: str = EMBEDDING_PATH_ARGUMENT,
    output_metrics_path: str = METRICS_OUTPUT_ARGUMENT,
    output_pairwise_path: str = PAIRWISE_SCORE_OUTPUT_OPTION,
    config_path: str | None = EVAL_CONFIG_OPTION,
) -> None:
    """
    Evaluate metapath2vec embeddings using retrieval metrics.

    Computes NDCG, Recall, and Precision at different K values and relevance thresholds.
    Saves metrics CSV and pairwise scores parquet for further analysis.

    Example config file (config.json):
    {
        "n_samples": 2000,
        "n_retrieved": 5000,
        "k_values": [10, 50, 100],
        "relevance_thresholds": [0.5, 0.7, 0.9]
    }
    """

    # Load config
    eval_config = None
    if config_path is not None:
        typer.echo(f"Loading config from: {config_path}")
        with open(config_path) as f:
            eval_config = json.load(f)
        typer.echo(f"Config loaded: {eval_config}")
    else:
        typer.echo("Using default evaluation config")
        typer.echo(f"Default config: {DEFAULT_EVAL_CONFIG}")

    metrics_df, results_df = evaluate_embeddings(
        raw_data_parquet_path=raw_data_path,
        embedding_parquet_path=embedding_path,
        eval_config=eval_config,
    )
    # Save metrics

    metrics_df.to_csv(output_metrics_path, index=False)
    typer.secho(
        f"Metrics saved to: {output_metrics_path}",
    )

    # Save pairwise scores
    if output_pairwise_path:
        results_df.to_parquet(output_pairwise_path, index=False)
        typer.secho(f"Pairwise scores saved to: {output_pairwise_path}")


if __name__ == "__main__":
    app()
