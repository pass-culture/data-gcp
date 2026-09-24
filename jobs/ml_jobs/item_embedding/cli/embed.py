"""Step 3 of the embedding pipeline: embed prompts.

Reads a vector's prompts from GCS, loads its encoder, embeds each prompt, and
writes ``item_id, content_hash, embedding`` -- plus the provenance columns
``mlflow_run_id`` and ``embedding_model`` -- back to GCS for loading into
BigQuery. Over-length prompts (silently truncated by the encoder) are flagged
and summarized at the end of the run, which also logs this vector's config and
counts to the shared DAG-run MLflow run.

Run from the job root:
    uv run python -m cli.embed \
        --config-file-name movies_metadata \
        --input-parquets-folder-path  gs://.../movies_metadata/prompts \
        --output-parquets-folder-path gs://.../movies_metadata/embeddings
"""

import torch
import typer
from loguru import logger
from src.config import CONFIGS_PATH, load_vector_config
from src.constants import (
    EMBEDDING_MODEL_COLUMN,
    MLFLOW_RUN_ID_COLUMN,
    ROWS_PER_CHUNK,
)
from src.embedding import LongPromptTracker, encode, find_long_prompts
from src.gcs_utils import iter_parquet_chunks, write_embeddings_parquet
from src.mlflow_utils import log_vector_to_run, read_run_id
from src.setup_encoders import load_encoder, start_pool, stop_pool

app = typer.Typer(help="Embed prompts into vectors.")


def _gpu_count() -> int:
    return torch.cuda.device_count() if torch.cuda.is_available() else 0


@app.command()
def main(
    config_file_name: str = typer.Option(
        ..., help="Single-vector config name (without .yaml) in configs/"
    ),
    input_parquets_folder_path: str = typer.Option(
        ..., help="GCS folder with the vector's prompts parquet files"
    ),
    output_parquets_folder_path: str = typer.Option(
        ..., help="GCS folder to write the embeddings parquet files to"
    ),
    rows_per_chunk: int = typer.Option(ROWS_PER_CHUNK),
) -> None:
    vector = load_vector_config(config_file_name)
    # DAG-run run_id (from the mlflow_run start step): stamped onto every row
    # below and used to resume the run at the end. "" when running standalone.
    run_id = read_run_id()
    gpu_count = _gpu_count()
    logger.info(f"Embedding vector '{vector.name}' on {gpu_count} GPU(s)")

    encoder = load_encoder(vector.encoder_name, gpu_count)
    pool = start_pool(encoder, gpu_count)
    tracker = LongPromptTracker()
    total_embedded = 0
    try:
        for i, chunk in enumerate(
            iter_parquet_chunks(
                input_parquets_folder_path,
                rows_per_chunk,
                required_columns=["item_id", "content_hash", "prompt"],
            )
        ):
            prompts = chunk["prompt"].tolist()
            find_long_prompts(
                vector, encoder, chunk["item_id"].tolist(), prompts, tracker
            )
            embeddings = encode(encoder, prompts, vector.prompt_name, pool)

            out = chunk[["item_id", "content_hash"]].copy()
            out["embedding"] = embeddings.tolist()
            out[MLFLOW_RUN_ID_COLUMN] = run_id
            out[EMBEDDING_MODEL_COLUMN] = vector.encoder_name
            output_path = f"{output_parquets_folder_path}/embeddings_{i}.parquet"
            write_embeddings_parquet(out, output_path)
            total_embedded += len(out)
            logger.info(f"Wrote {len(out)} embeddings to {output_path}")
    finally:
        stop_pool(encoder, pool)

    tracker.log_summary()
    config_path = str(CONFIGS_PATH / f"{config_file_name}.yaml")
    log_vector_to_run(
        run_id, vector, config_path, total_embedded, len(tracker.long_item_ids)
    )
    logger.info("✅ Embedding complete")


if __name__ == "__main__":
    app()
