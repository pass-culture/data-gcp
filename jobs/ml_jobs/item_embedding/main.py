import typer
from database import load_parquet, upload_parquet
from embed_items import embed_all_vectors, load_config, parse_vectors
from loguru import logger

app = typer.Typer()


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
    logger.info("Starting embedding process")

    ## Load configuration and vectors
    config = load_config(config_file_name)
    vectors = parse_vectors(config)

    ## Load item metadata, generate embeddings, and save results
    df_items_metadata = load_parquet(input_parquet_filename)
    df_embeddings = embed_all_vectors(df_items_metadata, vectors, batch_size)
    upload_parquet(df_embeddings, output_parquet_filename)
    logger.info("Embedding process completed successfully")


if __name__ == "__main__":
    app()
