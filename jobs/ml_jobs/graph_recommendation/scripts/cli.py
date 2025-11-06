"""Command line interface for building book metadata graphs."""

from __future__ import annotations

import os

import mlflow
import pandas as pd
import torch
import typer

from src.config import EvaluationConfig, TrainingConfig
from src.constants import MLFLOW_RUN_ID_FILEPATH, RESULTS_DIR
from src.embedding_builder import train_metapath2vec
from src.evaluation import (
    evaluate_embeddings,
)
from src.graph_builder import (
    build_book_metadata_graph,
)
from src.heterograph_builder import build_book_metadata_heterograph
from src.utils.graph_stats import get_graph_analysis
from src.utils.mlflow import (
    MLflowAuthManager,
    conditional_mlflow,
    get_mlflow_experiment,
    log_detailed_scores,
    log_evaluation_metrics,
    log_graph_analysis,
    mlflow_token_refresher_context,
)

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
    "--output-graph",
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

TRAIN_CONFIG_OPTION = typer.Option(
    None,
    "--config",
    "-c",
    help="JSON string to override default training parameters.",
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

REBUILD_GRAPH_OPTION = typer.Option(
    False,
    "--rebuild-graph",
    help="Optional flag to rebuild graph if it already exists",
    show_default=True,
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


@conditional_mlflow()
def lazy_graph_building(
    parquet_path: str,
    results_dir: str = RESULTS_DIR,
    nrows: int | None = None,
    *,
    rebuild_graph: bool = False,
) -> tuple[object, pd.DataFrame, pd.DataFrame]:
    """
    Lazily builds or loads a graph and its analysis.

    Parameters:
        parquet_path: Path to the source parquet data
        results_dir: Directory to save graph and analysis
        rebuild_graph: Force rebuild if True
        nrows: Optional number of rows to read from parquet

    Returns:
        graph_data: The loaded or built graph
        graph_summary: Summary DataFrame
        graph_components: Components DataFrame
    """
    graph_path = f"{results_dir}/book_metadata_graph.pt"
    summary_path = f"{results_dir}/graph_summary.csv"
    components_path = f"{results_dir}/graph_components.csv"

    rebuild_needed = rebuild_graph or not os.path.exists(graph_path)

    if rebuild_needed:
        # Build graph and save
        graph_data = build_book_metadata_heterograph(parquet_path, nrows=nrows)
        torch.save(graph_data, graph_path)

        # Analyze and log
        graph_summary, graph_components = get_graph_analysis(graph_data)
        log_graph_analysis(graph_summary, graph_components)

    else:
        # Load graph
        graph_data = torch.load(graph_path, weights_only=False)

        # Load analysis, regenerate only if missing
        if os.path.exists(summary_path) and os.path.exists(components_path):
            graph_summary = pd.read_csv(summary_path)
            graph_components = pd.read_csv(components_path)
        else:
            graph_summary, graph_components = get_graph_analysis(graph_data)
        log_graph_analysis(graph_summary, graph_components)

    return graph_data


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
    nrows: int | None = NROWS_OPTION,
    config_json: str | None = TRAIN_CONFIG_OPTION,
    *,
    rebuild_graph: bool = REBUILD_GRAPH_OPTION,
):
    """Train a Metapath2Vec model on the book-to-metadata graph and save it to disk."""

    # Load default config
    training_config = (
        TrainingConfig().parse_and_update_config(config_json)
        if config_json
        else TrainingConfig()
    )
    typer.echo(f"Using training config: {training_config.to_dict()}", err=True)

    # Connect to MLflow
    mlflow_auth = MLflowAuthManager()
    mlflow_auth.authenticate()
    experiment = get_mlflow_experiment(experiment_name)

    with (
        mlflow_token_refresher_context(auth_manager=mlflow_auth),
        mlflow.start_run(experiment_id=experiment.experiment_id) as run,
    ):
        run_id = run.info.run_id
        typer.secho(
            f"Started MLflow run '{run_id}' in experiment '{experiment.name}'."
            f" Logging run_id to {MLFLOW_RUN_ID_FILEPATH}",
            fg=typer.colors.CYAN,
        )
        with open(MLFLOW_RUN_ID_FILEPATH, "w") as f:
            f.write(run_id)

        # Graph building
        graph_data = lazy_graph_building(
            parquet_path,
            nrows=nrows,
            results_dir=RESULTS_DIR,
            rebuild_graph=rebuild_graph,
        )

        # Train model with loss logging to mlflow
        embeddings_df = train_metapath2vec(
            graph_data=graph_data,
            training_config=training_config,
        )

        # Save embeddings
        embeddings_df.to_parquet(embedding_output_path, index=False)
        typer.secho(
            f"Embeddings saved to {embedding_output_path}",
            fg=typer.colors.GREEN,
        )


@app.command("evaluate-metapath2vec")
def evaluate_metapath2vec_command(
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

    evaluation_config = (
        EvaluationConfig().parse_and_update_config(config_json)
        if config_json
        else EvaluationConfig()
    )
    typer.echo(f"Using evaluation config: {evaluation_config.to_dict()}", err=True)

    # Retrieving run_id locally
    with open(MLFLOW_RUN_ID_FILEPATH) as f:
        run_id = f.read().strip()

    # Connect to MLflow
    mlflow_auth = MLflowAuthManager()
    mlflow_auth.authenticate()

    with (
        mlflow_token_refresher_context(auth_manager=mlflow_auth),
        mlflow.start_run(run_id=run_id),
    ):
        # Log config
        mlflow.log_params(
            {f"eval_{k}": v for k, v in evaluation_config.to_dict().items()}
        )

        # Evaluate embeddings
        metrics_df, results_df = evaluate_embeddings(
            raw_data_parquet_path=raw_data_path,
            embedding_parquet_path=embedding_path,
            evaluation_config=evaluation_config,
        )

        # Log metrics to MLflow
        log_evaluation_metrics(
            metrics_df,
            output_metrics_path,
        )

        # Log detailed scores if requested
        if output_detailed_scores_path:
            log_detailed_scores(
                results_df,
                output_detailed_scores_path,
            )

    typer.secho(
        f"✓ Metrics logged to MLflow run: {run_id}",
        fg=typer.colors.CYAN,
        err=True,
    )


if __name__ == "__main__":
    app()
