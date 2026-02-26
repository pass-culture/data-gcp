import time

import typer
from config import load_config, parse_vectors
from embed_items import embed_all_vectors
from loguru import logger
from storage import load_parquet, upload_parquet

app = typer.Typer(
    help="Generate item embeddings using Hugging Face models and save results to GCS."
)


@app.command()
def main(
    config_file_name: str = typer.Option("default"),
    batch_size: int = typer.Option(
        100,
        help="Number of items to process per batch",
    ),
    input_parquet_filename: str = typer.Option(
        ...,
        help="Name of the input dataset",
    ),
    output_parquet_filename: str = typer.Option(
        ...,
        help="Name of the output dataset",
    ),
) -> None:
    """Main function to load item metadata, generate embeddings, and save results.

    Args:
        config_file_name: Name of the configuration file (without .yaml extension)
        batch_size: Number of items to process per batch
        input_parquet_filename: Path to the input parquet file containing item metadata on GCS
        output_parquet_filename: Path to the output parquet file on GCS where results will be saved
    """
    logger.info(
        f"Starting embedding process with the following parameters:\n"
        f"  Config file: {config_file_name}\n"
        f"  Batch size: {batch_size}\n"
        f"  Input parquet file: {input_parquet_filename}\n"
        f"  Output parquet file: {output_parquet_filename}"
    )
    total_start = time.perf_counter()

    # Load configuration and vectors
    config = load_config(config_file_name)
    vectors = parse_vectors(config)

    # Load item metadata
    t0 = time.perf_counter()
    df_items_metadata = load_parquet(input_parquet_filename)
    logger.info(f"Data loading took {time.perf_counter() - t0:.1f}s")

    # Generate embeddings
    t0 = time.perf_counter()
    df_embeddings = embed_all_vectors(df_items_metadata, vectors, batch_size)
    logger.info(f"Embedding took {time.perf_counter() - t0:.1f}s")

    # Upload results
    t0 = time.perf_counter()
    upload_parquet(df_embeddings, output_parquet_filename)
    logger.info(f"Upload took {time.perf_counter() - t0:.1f}s")

    logger.info(
        f"Embedding process completed successfully in {time.perf_counter() - total_start:.1f}s"
    )


if __name__ == "__main__":
    app()
