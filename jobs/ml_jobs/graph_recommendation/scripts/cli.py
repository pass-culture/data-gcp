"""Command line interface for building book metadata graphs."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import mlflow
import torch
import typer

from src.constants import RESULTS_DIR
from src.embedding_builder import train_metapath2vec
from src.evaluation import (
    DEFAULT_EVAL_CONFIG,
    evaluate_embeddings,
    log_metrics_at_k_csv,
)
from src.graph_builder import (
    build_book_metadata_graph,
)
from src.heterograph_builder import build_book_metadata_heterograph
from src.utils.commons import connect_remote_mlflow, get_mlflow_experiment
from src.utils.graph_stats import get_graph_analysis

APP_DESCRIPTION = (
    "Utilities to build PyTorch Geometric graphs for book recommendations."
)
app = typer.Typer(help=APP_DESCRIPTION)

EXPERIMENT_NAME_ARGUMENT = typer.Argument(
    ...,
    help="Name of the experiment. for mlflow tracking.",
)

RUN_ID_ARGUMENT = typer.Argument(
    ...,
    help="Name of the run. for mlflow tracking.",
)

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

EMBEDDING_INPUT_ARGUMENT = typer.Argument(
    ...,
    help="Where to save the node embeddings as a parquet file. "
    "Can be a local path or a GCS path (gs://...).",
    dir_okay=False,
)

METRICS_OUTPUT_ARGUMENT = typer.Argument(
    ...,
    help="Where to save the evaluation metrics CSV file.",
    dir_okay=False,
)

DETAILED_SCORE_OUTPUT_OPTION = typer.Option(
    None,
    "--output-scores-path",
    help="Where to save the pairwise scores parquet file.",
    dir_okay=False,
)

EVAL_CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    help="JSON string to override default evaluation parameters.",
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
    experiment_name: str = EXPERIMENT_NAME_ARGUMENT,
    parquet_path: str = PARQUET_ARGUMENT,
    embedding_output_path: str = EMBEDDING_OUTPUT_OPTION,
    num_workers: int = NUM_WORKERS_OPTION,
    nrows: int | None = NROWS_OPTION,
) -> None:
    """Train a Metapath2Vec model on the book-to-metadata graph and save it to disk."""

    # Connect to MLflow
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name)

    # Start MLflow run
    with mlflow.start_run(experiment_id=experiment.experiment_id) as run:
        run_id = run.info.run_id
        typer.secho(
            f"Started MLflow run '{run_id}' in experiment '{experiment.name}'",
            fg=typer.colors.CYAN,
            err=True,
        )
        # Build graph
        graph_data = build_book_metadata_heterograph(
            parquet_path,
            nrows=nrows,
        )

        # Log graph statistics
        graph_summary, graph_components = get_graph_analysis(graph_data)
        with tempfile.TemporaryDirectory() as tmpdir:
            local_graph_path = Path(tmpdir) / "graph_components.csv"
            graph_components.to_csv(local_graph_path, index=False)
            mlflow.log_artifact(str(local_graph_path), artifact_path=None)
        with tempfile.TemporaryDirectory() as tmpdir:
            local_graph_path = Path(tmpdir) / "graph_summary.csv"
            graph_summary.to_csv(local_graph_path, index=False)
            mlflow.log_artifact(str(local_graph_path), artifact_path=None)

        # Train model with loss logging to mlflow
        embeddings_df = train_metapath2vec(
            graph_data=graph_data,
            num_workers=num_workers,
        )

        # Save embeddings
        embeddings_df.to_parquet(embedding_output_path, index=False)

        # Use err=True to send messages to stderr (won't interfere with XCom)
        typer.secho(
            f"Embeddings saved to {embedding_output_path}",
            fg=typer.colors.GREEN,
            err=True,
        )
        typer.echo(f"MLflow run_id: {run_id}", err=True)

        # Return run_id - Typer will print it to stdout for xcom capture
        return run_id


@app.command("evaluate-metapath2vec")
def evaluate_metapath2vec_command(
    run_id: str = RUN_ID_ARGUMENT,
    raw_data_path: str = PARQUET_ARGUMENT,
    embedding_path: str = EMBEDDING_INPUT_ARGUMENT,
    output_metrics_path: str = METRICS_OUTPUT_ARGUMENT,
    output_detailed_scores_path: str | None = DETAILED_SCORE_OUTPUT_OPTION,
    config_json: str | None = EVAL_CONFIG_OPTION,
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

    # Connect to MLflow
    connect_remote_mlflow()

    # Load default config
    eval_config = DEFAULT_EVAL_CONFIG.copy()
    # Override with user config if provided
    if config_json:
        try:
            config_dict = json.loads(config_json)
        except json.JSONDecodeError as e:
            raise typer.BadParameter(f"Invalid JSON: {e}") from e
        eval_config.update(config_dict)

    typer.echo(f"Using evaluation config: {eval_config}", err=True)

    # Resume the run
    with mlflow.start_run(run_id=run_id):
        # Log config
        mlflow.log_params({f"eval_{k}": v for k, v in eval_config.items()})

        # Evaluate embeddings
        metrics_df, results_df = evaluate_embeddings(
            raw_data_parquet_path=raw_data_path,
            embedding_parquet_path=embedding_path,
            eval_config=eval_config,
        )

        # Log metrics atk with k as x-axis
        # Log threshold-dependent metrics (recall and precision)
        for _, row in metrics_df.iterrows():
            metric_suffix = f"thresh_{row['threshold']}__{row['score_col']}"
            mlflow.log_metric(
                f"recall_at_k__{metric_suffix}", row["recall"], step=int(row["k"])
            )
            mlflow.log_metric(
                f"precision_at_k__{metric_suffix}",
                row["precision"],
                step=int(row["k"]),
            )

        # Log NDCG
        ndcg_metrics = metrics_df.drop_duplicates(subset=["score_col", "k"])
        for _, row in ndcg_metrics.iterrows():
            mlflow.log_metric(
                f"ndcg_at_k__{row['score_col']}",
                row["ndcg"],
                step=int(row["k"]),
            )

        # Log metrics artifact for easy lookup
        log_metrics_at_k_csv(
            metrics_df, order_by=["score_col", "threshold", "metric", "k"]
        )

        typer.secho(
            f"Metrics saved to: {output_metrics_path}",
            fg=typer.colors.GREEN,
            err=True,
        )

        # Save detailed scores locally first (mandatory for csv)
        filename = output_metrics_path.split("/")[-1]
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / filename
            metrics_df.to_csv(local_path, index=False)

            # Then log as MLflow artifact
            mlflow.log_artifact(str(local_path), artifact_path=None)

        # Save detailed scores if requested
        if output_detailed_scores_path:
            results_df.to_parquet(output_detailed_scores_path, index=False)
            typer.secho(
                f"Detailed query scores saved to: {output_detailed_scores_path}",
                fg=typer.colors.GREEN,
                err=True,
            )

        typer.secho(
            f"✓ Metrics logged to MLflow run: {run_id}",
            fg=typer.colors.CYAN,
            err=True,
        )


if __name__ == "__main__":
    app()
