import logging
import os
import time
from datetime import datetime
from typing import List
from urllib.parse import urlparse

import duckdb
import psycopg2
from google.cloud import storage
from psycopg2.extensions import connection

from config import TableConfig
from utils import (
    PROJECT_NAME,
    RECOMMENDATION_SQL_INSTANCE,
    access_secret_data,
)

logger = logging.getLogger(__name__)

# Constants for processing
MAX_RETRIES = 5


def get_db_connection() -> connection:
    """Create a database connection with retries."""
    database_url = access_secret_data(
        PROJECT_NAME,
        f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
    )

    retry_count = 0
    conn = None

    while retry_count < MAX_RETRIES and conn is None:
        try:
            conn = psycopg2.connect(database_url)
            conn.autocommit = False
            return conn
        except Exception as e:
            retry_count += 1
            if retry_count >= MAX_RETRIES:
                logger.error(
                    f"Failed to connect to database after {MAX_RETRIES} attempts: {str(e)}"
                )
                raise

            wait_time = min(30, 5 * retry_count)
            logger.warning(
                f"Database connection failed (attempt {retry_count}/{MAX_RETRIES}). Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)


def download_and_process_parquet_files(bucket_path: str, table_name: str) -> List[str]:
    """
    Download Parquet files from GCS and return their local paths.

    Args:
        bucket_path: GCS bucket path (gs://bucket-name/path)
        table_name: Name of the table to import

    Returns:
        List of local file paths to downloaded Parquet files
    """
    # Parse bucket path
    parsed_url = urlparse(bucket_path)
    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip("/")

    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # List all blobs matching the pattern
    blobs = list(bucket.list_blobs(prefix=f"{prefix}/{table_name}-"))

    # Filter for Parquet files
    parquet_blobs = [blob for blob in blobs if blob.name.endswith(".parquet")]

    if not parquet_blobs:
        raise ValueError(
            f"No Parquet files found for table {table_name} at {bucket_path}/{table_name}-*.parquet"
        )

    logger.info(f"Found {len(parquet_blobs)} Parquet files for table {table_name}")

    # Create temporary directory for downloaded files if it doesn't exist
    temp_dir = "/tmp/parquet_imports"
    os.makedirs(temp_dir, exist_ok=True)

    # Download files
    local_files = []
    for blob in parquet_blobs:
        local_path = os.path.join(temp_dir, os.path.basename(blob.name))
        blob.download_to_filename(local_path)
        local_files.append(local_path)
        logger.info(f"Downloaded {blob.name} to {local_path}")

    return local_files


def import_table_to_sql(
    table_name: str,
    table_config: TableConfig,
    bucket_path: str,
    execution_date: datetime,
) -> None:
    """Import a table from GCS Parquet files to Cloud SQL using DuckDB as an intermediary.

    Args:
        table_name: Name of the table to import
        table_config: Configuration for the table
        bucket_path: Full GCS path where the export is stored
        execution_date: Execution date for the import
    """
    logger.info(f"Starting import of {table_name} to Cloud SQL using DuckDB")
    start_time = time.time()

    try:
        # Download Parquet files from GCS
        parquet_files = download_and_process_parquet_files(bucket_path, table_name)

        # Get PostgreSQL connection details
        database_url = access_secret_data(
            PROJECT_NAME,
            f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
        )

        # Create a temporary DuckDB database
        duck_db_path = f"/tmp/{table_name}_{execution_date.strftime('%Y%m%d')}.duckdb"

        # Initialize DuckDB connection
        duck_conn = duckdb.connect(duck_db_path)

        try:
            # Install and load required extensions
            logger.info("Installing and loading required DuckDB extensions")
            duck_conn.execute("INSTALL postgres")
            duck_conn.execute("LOAD postgres")
            duck_conn.execute("INSTALL spatial")
            duck_conn.execute("LOAD spatial")

            # Attach PostgreSQL database
            duck_conn.execute(f"ATTACH '{database_url}' AS pg_db (TYPE postgres)")

            # Create a view of all Parquet files
            parquet_files_str = ", ".join([f"'{file}'" for file in parquet_files])
            logger.info("Creating DuckDB view from Parquet files")
            duck_conn.execute(
                f"CREATE VIEW parquet_data AS SELECT * FROM parquet_scan([{parquet_files_str}])"
            )

            # Get column definitions for PostgreSQL table
            columns_sql = table_config.column_definitions

            # Drop existing table in PostgreSQL if it exists
            logger.info(f"Dropping existing table {table_name} if it exists")
            duck_conn.execute(f"DROP TABLE IF EXISTS pg_db.{table_name}")

            # Create table in PostgreSQL
            logger.info(f"Creating table {table_name} in PostgreSQL")
            create_table_sql = f"CREATE TABLE pg_db.{table_name} ({columns_sql})"
            duck_conn.execute(create_table_sql)

            # Create view with proper column conversions
            logger.info("Creating view with column conversions")
            duck_conn.execute(
                f"CREATE VIEW parquet_data_wkt AS SELECT {table_config.duckdb_select_columns} FROM parquet_data"
            )

            # Copy data from Parquet to PostgreSQL using the WKT view
            logger.info(f"Copying data from Parquet to PostgreSQL table {table_name}")
            copy_start_time = time.time()
            duck_conn.execute(
                f"INSERT INTO pg_db.{table_name} SELECT * FROM parquet_data_wkt"
            )

            copy_elapsed_time = time.time() - copy_start_time

            # Get row count
            result = duck_conn.execute(
                f"SELECT COUNT(*) FROM pg_db.{table_name}"
            ).fetchone()
            total_rows = result[0] if result else 0

            logger.info(
                f"Successfully copied {total_rows} rows to PostgreSQL table {table_name} in {copy_elapsed_time:.2f} seconds"
            )

            # Detach PostgreSQL database
            duck_conn.execute("DETACH pg_db")

        finally:
            # Close DuckDB connection
            duck_conn.close()

            # Remove temporary DuckDB database
            if os.path.exists(duck_db_path):
                os.remove(duck_db_path)

        # Clean up downloaded Parquet files
        for parquet_file in parquet_files:
            if os.path.exists(parquet_file):
                os.remove(parquet_file)

        elapsed_time = time.time() - start_time
        logger.info(
            f"Successfully imported {table_name} to Cloud SQL in {elapsed_time:.2f} seconds"
        )

    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(
            f"Failed to import {table_name} to Cloud SQL after {elapsed_time:.2f} seconds: {str(e)}"
        )
        raise
    finally:
        # Clean up any remaining temporary files
        for parquet_file in parquet_files if "parquet_files" in locals() else []:
            if os.path.exists(parquet_file):
                os.remove(parquet_file)
