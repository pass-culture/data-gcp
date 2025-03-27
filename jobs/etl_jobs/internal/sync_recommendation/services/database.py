import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from google.cloud import bigquery

from jobs.etl_jobs.internal.sync_recommendation.utils.db import get_db_connection

logger = logging.getLogger(__name__)


class DatabaseService(ABC):
    """Abstract base class for database operations"""

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a database query"""
        pass

    @abstractmethod
    def export_data(self, query: str, destination_path: str) -> None:
        """Export data to a destination"""
        pass

    @abstractmethod
    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query"""
        pass


class BigQueryService(DatabaseService):
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self._last_query_result = None

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        job_config = bigquery.QueryJobConfig(
            destination=params.get("destination"),
            write_disposition=params.get("write_disposition", "WRITE_TRUNCATE"),
        )
        query_job = self.client.query(query, job_config=job_config)
        self._last_query_result = query_job.result()

    def export_data(
        self, source_ref: str, destination_uri: str, format: str = "PARQUET"
    ) -> None:
        """Export BigQuery table to GCS"""
        job_config = bigquery.ExtractJobConfig(
            destination_format=format,
            compression="SNAPPY",
        )
        extract_job = self.client.extract_table(
            source_ref,
            destination_uri,
            job_config=job_config,
        )
        extract_job.result()

    def load_from_gcs(
        self,
        table_id: str,
        gcs_path: str,
        schema: Optional[List[bigquery.SchemaField]] = None,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        """
        Load data from GCS Parquet files into a BigQuery table.

        Args:
            table_id: Full table ID (project.dataset.table)
            gcs_path: GCS path containing Parquet files
            schema: Optional schema for the table. If None, will be inferred from Parquet files
            write_disposition: Write disposition for the load job (WRITE_APPEND, WRITE_TRUNCATE, etc.)
        """
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            schema=schema,
            # Parquet-specific options
            parquet_options=bigquery.ParquetOptions(
                enable_list_inference=True,  # Enable inference for LIST and STRUCT types
                enum_as_string=True,  # Convert ENUM types to strings
            ),
        )

        # Start the load job
        load_job = self.client.load_table_from_uri(
            source_uris=gcs_path,
            destination=table_id,
            job_config=job_config,
        )

        # Wait for the job to complete
        load_job.result()

        # Get the number of rows loaded
        table = self.client.get_table(table_id)
        logger.info(f"Loaded {table.num_rows} rows into {table_id}")

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query"""
        if not self._last_query_result:
            raise RuntimeError("No query has been executed yet")
        return next(iter(self._last_query_result))


class CloudSQLService(DatabaseService):
    def __init__(self, connection_params: Dict):
        self.connection_params = connection_params
        self._last_cursor = None
        self.duck_service = DuckDBService()

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        with get_db_connection(**self.connection_params) as conn:
            with conn.cursor() as cursor:
                self._last_cursor = cursor
                cursor.execute(query, params or {})
                conn.commit()

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query"""
        if not self._last_cursor:
            raise RuntimeError("No query has been executed yet")
        return self._last_cursor.fetchone()


class DuckDBService(DatabaseService):
    def __init__(self, database_path: str = ":memory:"):
        self.database_path = database_path
        self._last_result = None

    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self.database_path)
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute("INSTALL spatial; LOAD spatial;")
        return conn

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        with self.setup_connection() as conn:
            self._last_result = conn.execute(query).fetchall()

    def parquet_to_cloudsql(self, query: str, destination_path: str) -> None:
        """Export data from DuckDB"""
        with self.setup_connection() as conn:
            conn.execute(
                f"COPY ({query}) TO '{destination_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
        logger.info(f"Successfully exported data to {destination_path}")

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query"""
        if not self._last_result:
            raise RuntimeError("No query has been executed yet")
        return self._last_result[0]

    def cloudsql_to_parquet(self, query: str, destination_path: str) -> None:
        """
        Export data from CloudSQL to Parquet using DuckDB.

        Args:
            query: SQL query to extract data from CloudSQL
            destination_path: Local path where to save the Parquet file
        """
        # Setup DuckDB connection with PostgreSQL extension
        with self.setup_connection() as conn:
            conn.execute(f"""
                COPY (
                    SELECT * FROM pg_db.({query})
                ) TO '{destination_path}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)
        logger.info(f"Successfully exported data to {destination_path}")
