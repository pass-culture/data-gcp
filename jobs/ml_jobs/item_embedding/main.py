import torch
import typer
from config import parse_vector
from embedding import embed_dataframe
from gcs_utils import list_parquet_files, load_parquet_file
from loguru import logger
from setup_encoders import (
    load_encoder,
    start_encoder_pool,
    stop_encoder_pool,
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
) -> None:
    """Main function to load item metadata, generate embeddings, and save results as parquets.

    Args:
        config_file_name: Name of the configuration file (without .yaml extension)
        input_parquets_folder_path: Path to the input parquet files containing item metadata on GCS
        output_parquets_folder_path: Path to the output parquet folder on GCS where results will be saved
    """
    logger.info(
        f"Starting embedding process with the following parameters:\n"
        f"  Config file: {config_file_name}\n"
        f"  Input parquets folder path: {input_parquets_folder_path}\n"
        f"  Output parquets folder path: {output_parquets_folder_path}"
    )
    # Load vector configuration and encoder weights
    vector = parse_vector(config_file_name)

    gpu_count = _get_gpu_count()
    logger.info(f"Detected {gpu_count} GPU(s) available")

    encoder = load_encoder(vector, gpu_count)

    ## List all parquet files matching the input path
    parquet_files = list_parquet_files(input_parquets_folder_path)
    logger.info(f"Found {len(parquet_files)} parquet files to process")

    # Start the multi-GPU pool once for the whole run if available
    pool = start_encoder_pool(encoder, gpu_count)
    try:
        for i, parquet_filepath in enumerate(parquet_files):
            logger.info(
                f"Processing parquet file {i + 1}/{len(parquet_files)}: {parquet_filepath}"
            )

            df_metadata = load_parquet_file(parquet_filepath, vector)

            df_embeddings = embed_dataframe(df_metadata, vector, encoder, pool=pool)
            logger.info(
                f"Generated embeddings for {len(df_embeddings)} items from {parquet_filepath}"
            )

            output_parquet_path = (
                f"{output_parquets_folder_path}/item_embeddings_{i}.parquet"
            )
            df_embeddings.to_parquet(output_parquet_path, index=False)
            logger.info(f"Saved embeddings to {output_parquet_path}")
    finally:
        stop_encoder_pool(encoder, pool)

    logger.info("✅ All parquet files processed successfully")


if __name__ == "__main__":
    app()
