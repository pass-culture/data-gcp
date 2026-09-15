import torch
import typer
from config import parse_vectors
from constants import ROWS_PER_CHUNK
from embedding import LongPromptTracker, embed_dataframe
from gcs_utils import iter_metadata_chunks, list_parquet_files, write_embeddings_parquet
from loguru import logger
from setup_encoders import (
    load_encoders,
    start_encoder_pools,
    stop_encoder_pools,
)

app = typer.Typer(
    help="Generate item embeddings using Hugging Face models and save results to GCS."
)


def _get_gpu_count() -> int:
    """Return the number of available CUDA GPUs."""
    return torch.cuda.device_count() if torch.cuda.is_available() else 0


@app.command()
def main(
    config_file_name: str = typer.Option("default"),
    input_parquets_folder_path: str = typer.Option(
        ...,
        help="Path to the input parquet files containing item metadata on GCS",
    ),
    output_parquets_folder_path: str = typer.Option(
        ...,
        help="Path to the output parquet folder on GCS where results will be saved",
    ),
    rows_per_chunk: int = typer.Option(
        ROWS_PER_CHUNK,
        help="Target number of rows per uniform embedding chunk, streamed "
        "across all input parquet files regardless of how BigQuery sharded "
        "them (tune for GPU throughput).",
    ),
) -> None:
    """Main function to load item metadata, generate embeddings, and save results as parquets.

    Args:
        config_file_name: Name of the configuration file (without .yaml extension)
        input_parquets_folder_path: Path to the input parquet files containing item metadata on GCS
        output_parquets_folder_path: Path to the output parquet folder on GCS where results will be saved
        rows_per_chunk: Target number of rows per uniform embedding chunk
    """
    logger.info(
        f"Starting embedding process with the following parameters:\n"
        f"  Config file: {config_file_name}\n"
        f"  Input parquets folder path: {input_parquets_folder_path}\n"
        f"  Output parquets folder path: {output_parquets_folder_path}"
    )
    # Load vectors configuration and encoder weights
    vectors = parse_vectors(config_file_name)

    gpu_count = _get_gpu_count()
    logger.info(f"Detected {gpu_count} GPU(s) available")

    encoders = load_encoders(vectors, gpu_count)

    ## List all parquet files matching the input path (for visibility only;
    ## iter_metadata_chunks streams across them as one unified dataset).
    parquet_files = list_parquet_files(input_parquets_folder_path)
    logger.info(
        f"Found {len(parquet_files)} parquet files to process, "
        f"streaming as uniform chunks of up to {rows_per_chunk} rows"
    )

    # Start multi-GPU pools once for the whole run if available
    pools = start_encoder_pools(encoders, gpu_count)
    # Shared across every chunk so over-length prompts are reported once, at
    # the end of the whole run, instead of scattered per chunk.
    long_prompt_tracker = LongPromptTracker()
    try:
        chunks = iter_metadata_chunks(
            input_parquets_folder_path, vectors, rows_per_chunk=rows_per_chunk
        )
        for i, df_metadata in enumerate(chunks):
            logger.info(f"Processing chunk {i + 1} ({len(df_metadata)} items)")

            df_embeddings = embed_dataframe(
                df_metadata,
                vectors,
                encoders,
                pools=pools,
                tracker=long_prompt_tracker,
            )
            logger.info(
                f"Generated embeddings for {len(df_embeddings)} items in chunk {i + 1}"
            )

            output_parquet_path = (
                f"{output_parquets_folder_path}/item_embeddings_{i}.parquet"
            )
            write_embeddings_parquet(df_embeddings, vectors, output_parquet_path)
            logger.info(f"Saved embeddings to {output_parquet_path}")
    finally:
        stop_encoder_pools(encoders, pools)

    long_prompt_tracker.log_summary()
    logger.info("✅ All parquet files processed successfully")


if __name__ == "__main__":
    app()
