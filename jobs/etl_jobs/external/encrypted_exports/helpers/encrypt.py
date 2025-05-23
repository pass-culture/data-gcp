import duckdb
import time
from typing import List
from contextlib import contextmanager
from fsspec import filesystem

from helpers.utils import (
    logger,
)

ENCRYPTED_FOLDER = "tmp_encrypted_folder"


@contextmanager
def get_duckdb_connection(encryption_key: str):
    """
    Context manager for DuckDB connection with encryption key setup.

    Args:
        encryption_key (str): The encryption key to use for parquet encryption.

    Yields:
        duckdb.DuckDBPyConnection: Configured DuckDB connection.
    """
    conn = duckdb.connect()
    try:
        conn.execute(f"PRAGMA add_parquet_key('key256', '{encryption_key}');")
        conn.register_filesystem(filesystem("gcs"))
        yield conn
    finally:
        conn.close()


def process_encryption(
    partner_name: str,
    gcs_bucket: str,
    export_date: str,
    table_list: List[str],
    encryption_key: str,
) -> None:
    """
    Encrypt parquet files directly from GCS to GCS using DuckDB.
    Combines all parquet files from a table into a single encrypted output file.

    Args:
        partner_name (str): Name of the partner.
        gcs_bucket (str): GCS bucket name.
        export_date (str): Export date.
        table_list (List[str]): List of tables to process.
        encryption_key (str): A 32-character encryption key.
    """

    with get_duckdb_connection(encryption_key) as conn:
        for table in table_list:
            try:
                start_time = time.time()

                # Define input and output paths
                input_folder = f"{partner_name}/{export_date}/{table}"
                output_folder = f"{ENCRYPTED_FOLDER}/{partner_name}/{export_date}/"

                # Create input and output GCS paths
                input_gcs_path = f"gs://{gcs_bucket}/{input_folder}/*.parquet"
                output_gcs_path = f"gs://{gcs_bucket}/{output_folder}/{table}.parquet"

                # Execute encryption query to combine all parquet files
                query = f"""
                COPY (SELECT * FROM read_parquet('{input_gcs_path}'))
                TO '{output_gcs_path}'
                (FORMAT 'parquet', ENCRYPTION_CONFIG {{footer_key: 'key256'}});
                """

                conn.execute(query)
                logger.info(
                    f"Encrypted all files from {input_folder} to {output_folder}/{table}.parquet"
                )

                duration = time.time() - start_time
                logger.success(
                    f"Table {table} completed: combined all files into single encrypted file in {duration:.2f}s"
                )

            except Exception as e:
                logger.error(f"Error processing table {table}: {e}")
                continue
