"""Embedding reduction: fetch corpus embeddings and fit/transform UMAP
(spec section 7.2).

CLI commands stay file-based (parquet in/out); the fitted model goes to GCS
via ``src.common.gcs.save_model``/``load_model``. Run from the job root:

    uv run python main.py reduction fetch-embeddings \\
        --documents-file-path documents.parquet --output-file-path embeddings.parquet \\
        --gtl-ids 01000000

    uv run python main.py reduction fit \\
        --input-file-path embeddings.parquet --output-file-path vectors.parquet \\
        --split-level 1 --corpus-id 01000000 --run-id abc123
"""

import numpy as np
import polars as pl
import typer
from loguru import logger

from src.common import gcs
from src.common.config import apply_overrides, load_config
from src.reduction.embeddings import (
    fetch_embeddings,
    join_with_documents,
    validate_embeddings,
)
from src.reduction.umap_models import fit_umap

app = typer.Typer(help="Embedding reduction: UMAP on corpus embeddings.")

MODEL_BLOB_NAME = "umap_model.pkl"


@app.command("fetch-embeddings")
def fetch_embeddings_command(
    documents_file_path: str = typer.Option(
        ...,
        help="Parquet of the corpus's non-empty topic_item_document rows "
        "(item_id, gtl_id).",
    ),
    output_file_path: str = typer.Option(
        ..., help="Where to write the joined, validated embeddings."
    ),
    gtl_ids: list[str] = typer.Option(  # noqa: B008
        ..., help="The corpus's GTL ids (repeat the option per id)."
    ),
    config_file_name: str = typer.Option("default"),
    dim: int | None = typer.Option(None),
) -> None:
    config = load_config(config_file_name)
    reduction_config = apply_overrides(config.reduction, {"dim": dim})

    documents = pl.read_parquet(documents_file_path)
    embeddings = fetch_embeddings(reduction_config.dim, gtl_ids)
    joined = join_with_documents(embeddings, documents)
    validate_embeddings(joined, joined["item_id"].to_list())

    joined.write_parquet(output_file_path)
    logger.info(f"Fetched {joined.height} embeddings -> {output_file_path}")


@app.command()
def fit(
    input_file_path: str = typer.Option(
        ..., help="Embeddings parquet from fetch-embeddings."
    ),
    output_file_path: str = typer.Option(
        ..., help="Where to write the 5-d vectors + item ids."
    ),
    split_level: int = typer.Option(..., help="This corpus's split level."),
    corpus_id: str = typer.Option(..., help="This corpus's id."),
    run_id: str = typer.Option(
        ..., help="This run's id (used in the model's GCS path)."
    ),
    config_file_name: str = typer.Option("default"),
    device: str = typer.Option(
        "gpu", help="'gpu' (cuML, with CPU fallback on OOM) or 'cpu'."
    ),
) -> None:
    config = load_config(config_file_name)
    data = pl.read_parquet(input_file_path)
    vectors = np.asarray(data["embedding"].to_list(), dtype=np.float32)

    model, reduced = fit_umap(vectors, config.reduction, device=device)

    model_path = gcs.model_path(split_level, corpus_id, run_id) + MODEL_BLOB_NAME
    gcs.save_model(model, model_path)
    gcs.write_latest_pointer(split_level, corpus_id, run_id)

    result = pl.DataFrame({"item_id": data["item_id"], "vector": reduced.tolist()})
    result.write_parquet(output_file_path)
    logger.info(
        f"Fit UMAP on {vectors.shape[0]} items ({config.reduction.n_components}-d) "
        f"-> {output_file_path}"
    )


@app.command()
def transform(
    input_file_path: str = typer.Option(
        ..., help="New items' embeddings parquet to project."
    ),
    output_file_path: str = typer.Option(..., help="Where to write the 5-d vectors."),
    split_level: int = typer.Option(..., help="This corpus's split level."),
    corpus_id: str = typer.Option(..., help="This corpus's id."),
    run_id: str = typer.Option(..., help="Which saved model's run to use."),
) -> None:
    model_path = gcs.model_path(split_level, corpus_id, run_id) + MODEL_BLOB_NAME
    model = gcs.load_model(model_path)

    data = pl.read_parquet(input_file_path)
    vectors = np.asarray(data["embedding"].to_list(), dtype=np.float32)
    reduced = model.transform(vectors)

    result = pl.DataFrame({"item_id": data["item_id"], "vector": reduced.tolist()})
    result.write_parquet(output_file_path)
    logger.info(f"Transformed {vectors.shape[0]} items -> {output_file_path}")


if __name__ == "__main__":
    app()
