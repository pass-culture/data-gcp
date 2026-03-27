import typer
from build_lancedb_table import build_lancedb_table


def main(
    gcs_embedding_parquet_file: str = typer.Option(
        help="GCS Parquet file or folder path"
    ),
    lancedb_uri: str = typer.Option(help="LanceDB URI"),
    lancedb_table: str = typer.Option(help="LanceDB table name"),
    batch_size: int = typer.Option(help="Batch size for streaming"),
    vector_column_name: str = typer.Option(
        help="Name of the vector column in the parquet file"
    ),
):
    """Create LanceDB table with item embeddings from GCS parquet files."""

    build_lancedb_table(
        gcs_embedding_parquet_file=gcs_embedding_parquet_file,
        lancedb_uri=lancedb_uri,
        lancedb_table=lancedb_table,
        batch_size=batch_size,
        vector_column_name=vector_column_name,
    )


if __name__ == "__main__":
    typer.run(main)
