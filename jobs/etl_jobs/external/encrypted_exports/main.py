import ast
import typer

from helpers.encrypt import process_encryption
from helpers.transfer import process_transfer
from helpers.utils import DEFAULT_BATCH_SIZE, DEFAULT_MAX_WORKERS

run = typer.Typer()


@run.command()
def encrypt(
    partner_name: str = typer.Option(..., help="Partner name"),
    gcs_bucket: str = typer.Option(..., help="GCS bucket name"),
    export_date: str = typer.Option(..., help="Export date"),
    table_list: str = typer.Option(..., help="String list of tables to encrypt"),
    encryption_key: str = typer.Option(..., help="Encryption key"),
    batch_size: int = typer.Option(DEFAULT_BATCH_SIZE, help="Batch size"),
    max_workers: int = typer.Option(DEFAULT_MAX_WORKERS, help="Max CPU workers"),
) -> None:
    """
    Encrypt parquet files stored in GCS for a given partner and list of tables.

    The function downloads each parquet file from the specified GCS bucket, encrypts it using
    DuckDB with the provided 32-character key, uploads the encrypted file back to GCS,
    and then cleans up the local temporary files.
    """
    table_list = ast.literal_eval(table_list)
    assert len(encryption_key) == 32, "Encryption key must be a string of 32 integers"

    process_encryption(
        partner_name,
        gcs_bucket,
        export_date,
        table_list,
        encryption_key,
        batch_size=batch_size,
        max_workers=max_workers,
    )


@run.command()
def transfer(
    partner_name: str = typer.Option(..., help="Partner name"),
    gcs_bucket: str = typer.Option(..., help="GCS bucket name"),
    export_date: str = typer.Option(..., help="Export date"),
    table_list: str = typer.Option(..., help="String list of tables to encrypt"),
    max_workers: int = typer.Option(DEFAULT_MAX_WORKERS, help="Max CPU workers"),
) -> None:
    """
    Transfer encrypted parquet files from GCS to an S3-compatible bucket.

    The function parses the target bucket configuration, initializes an S3 client, then for
    each table, it retrieves the encrypted parquet files from GCS and uploads them to the target S3 bucket.
    """
    table_list = ast.literal_eval(table_list)
    process_transfer(
        partner_name, gcs_bucket, export_date, table_list, max_workers=max_workers
    )


if __name__ == "__main__":
    run()
