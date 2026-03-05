import torch
import typer
from config import parse_vectors
from embedding import embed_dataframe, load_encoders
from gcs_utils import list_parquet_files, load_parquet_file
from loguru import logger

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
    """Main function to load item metadata, generate embeddings, and save results.

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
    # Load configuration and vectors
    vectors = parse_vectors(config_file_name)

    encoders = load_encoders(vectors)

    ## List all parquet files matching the input path
    parquet_files = list_parquet_files(input_parquets_folder_path)
    logger.info(f"Found {len(parquet_files)} parquet files to process")

    gpu_count = _get_gpu_count()
    logger.info(f"Detected {gpu_count} GPU(s) available")

    for i, parquet_filepath in enumerate(parquet_files):
        logger.info(
            f"Processing parquet file {i + 1}/{len(parquet_files)}: {parquet_filepath}"
        )

        # Load item metadata as a table
        df_metadata = load_parquet_file(parquet_filepath, vectors)

        # Stream batches, embed, and write to a local temp parquet
        df_embeddings = embed_dataframe(df_metadata, vectors, encoders, gpu_count)
        logger.info(
            f"Generated embeddings for {len(df_embeddings)} items from {parquet_filepath}"
        )
        # Upload the resulting dataframe to GCS as a parquet file
        output_parquet_path = (
            f"{output_parquets_folder_path}/item_embeddings_{i}.parquet"
        )
        df_embeddings.to_parquet(output_parquet_path, index=False)
        logger.info(f"Saved embeddings to {output_parquet_path}")

    logger.info("✔ All parquet files processed successfully")


if __name__ == "__main__":
    app()
