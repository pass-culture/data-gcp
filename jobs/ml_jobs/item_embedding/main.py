import os

import pyarrow as pa
import typer
from config import parse_vectors
from embedding import embed_parquet_file, load_encoders
from gcs_utils import list_parquet_files, load_parquet_file, upload_file_to_gcs
from loguru import logger

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
    input_parquets_path: str = typer.Option(
        ...,
        help="Path to the input parquet files containing item metadata on GCS",
    ),
    output_parquets_path: str = typer.Option(
        ...,
        help="Path to the output parquet file on GCS where results will be saved",
    ),
) -> None:
    """Main function to load item metadata, generate embeddings, and save results.

    Args:
        config_file_name: Name of the configuration file (without .yaml extension)
        batch_size: Number of items to process per batch
        input_parquets_path: Path to the input parquet files containing item metadata on GCS
        output_parquets_path: Path to the output parquet file on GCS where results will be saved
    """
    logger.info(
        f"Starting embedding process with the following parameters:\n"
        f"  Config file: {config_file_name}\n"
        f"  Batch size: {batch_size}\n"
        f"  Input parquets path: {input_parquets_path}\n"
        f"  Output parquets path: {output_parquets_path}"
    )
    # Load configuration and vectors
    vectors = parse_vectors(config_file_name)
    required_columns = list(
        set(
            ["item_id"] + [feature for vector in vectors for feature in vector.features]
        )
    )

    encoders = load_encoders(vectors)

    ## output parquet file schema
    output_schema = pa.schema(
        [("item_id", pa.string()), ("content_hash", pa.string())]
        + [
            (
                vector.name,
                pa.list_(
                    pa.float32(),
                    encoders[vector.encoder_name].get_sentence_embedding_dimension(),
                ),
            )
            for vector in vectors
        ]
    )
    ## List all parquet files matching the input path
    parquet_files = list_parquet_files(input_parquets_path)
    logger.info(f"Found {len(parquet_files)} parquet files to process")

    for i, parquet_filepath in enumerate(parquet_files):
        logger.info(
            f"Processing parquet file {i + 1}/{len(parquet_files)}: {parquet_filepath}"
        )

        # Load item metadata as a streamable ParquetFile
        pf = load_parquet_file(parquet_filepath, required_columns)

        # Stream batches, embed, and write to a local temp parquet
        local_tmp_path = embed_parquet_file(
            pf, vectors, encoders, output_schema, output_parquets_path, batch_size
        )

        # Build GCS destination: same basename under output_parquets_path
        basename = os.path.basename(parquet_filepath)
        gcs_dest = f"{output_parquets_path.rstrip('/')}/{basename}"

        # Upload completed parquet to GCS (also removes local temp file)
        upload_file_to_gcs(local_tmp_path, gcs_dest)

        logger.info(f"Done with parquet file {i + 1}/{len(parquet_files)}")

    logger.info("All parquet files processed successfully")


if __name__ == "__main__":
    app()
