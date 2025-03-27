import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import psycopg2
from google.cloud import bigquery
from psycopg2.extensions import connection

from utils.constant import MAX_RETRIES

logger = logging.getLogger(__name__)


class DatabaseService(ABC):
    """Abstract base class for database operations"""

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a database query"""
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
        """Execute a BigQuery query.

        Args:
            query: The SQL query to execute
            params: Optional dictionary containing:
                - destination: BigQuery table to write results to (optional)
                - write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY (default: WRITE_TRUNCATE)
        """
        if params and "destination" in params:
            job_config = bigquery.QueryJobConfig(
                destination=params["destination"],
                write_disposition=params.get("write_disposition", "WRITE_TRUNCATE"),
            )
            query_job = self.client.query(query, job_config=job_config)
        else:
            query_job = self.client.query(query)

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
        time_partitioning: Optional[bigquery.TimePartitioning] = None,
    ) -> None:
        """
        Load data from GCS Parquet files into a BigQuery table.

        Args:
            table_id: Full table ID (project.dataset.table)
            gcs_path: GCS path containing Parquet files
            schema: Optional schema for the table. If None, will be inferred from Parquet files
            write_disposition: Write disposition for the load job (WRITE_APPEND, WRITE_TRUNCATE, etc.)
            time_partitioning: Optional time partitioning for the table. If None, will not be partitioned.
        """
        logger.info(f"Loading data from {gcs_path} to {table_id}")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            schema=schema,
            time_partitioning=time_partitioning,
        )

        load_job = self.client.load_table_from_uri(
            source_uris=gcs_path,
            destination=table_id,
            job_config=job_config,
        )

        load_job.result()

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
        self._last_result = None
        self._connection = None
        self.duck_service = DuckDBService()

    def __get_db_connection(self) -> connection:
        """Create a database connection with retries."""
        retry_count = 0
        conn = None

        database_url = self.connection_params["database_url"]

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

    def _ensure_connection(self) -> None:
        """Ensure we have an active database connection."""
        if self._connection is None or self._connection.closed:
            self._connection = self.__get_db_connection()
            self._last_cursor = self._connection.cursor()

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a query on CloudSQL.

        Args:
            query: The SQL query to execute
            params: Optional query parameters
        """
        self._ensure_connection()
        self._last_cursor.execute(query, params or {})

        # Only try to fetch results for SELECT queries
        if query.strip().upper().startswith("SELECT"):
            self._last_result = self._last_cursor.fetchall()
        else:
            self._last_result = None

        self._connection.commit()

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query"""
        if not self._last_result:
            raise RuntimeError("No query has been executed yet")
        if not self._last_result:
            return None
        return self._last_result[0]

    def fetch_all(self) -> List[Tuple[Any, ...]]:
        """Fetch all rows from the last executed query"""
        if not self._last_result:
            raise RuntimeError("No query has been executed yet")
        return self._last_result

    def close(self) -> None:
        """Close the database connection and cursor."""
        if self._last_cursor:
            self._last_cursor.close()
            self._last_cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
        self._last_result = None


class DuckDBService(DatabaseService):
    def __init__(self, database_path: str = ":memory:"):
        self.database_path = database_path
        self._last_result = None
        self._connection = None

    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Setup a new DuckDB connection with required extensions."""
        if self._connection is None:
            self._connection = duckdb.connect(self.database_path)
            self._connection.execute("INSTALL postgres; LOAD postgres;")
            self._connection.execute("INSTALL httpfs; LOAD httpfs;")
            self._connection.execute("INSTALL spatial; LOAD spatial;")
        return self._connection

    def close_connection(self) -> None:
        """Close the DuckDB connection if it exists."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a query using the maintained connection."""
        conn = self.setup_connection()
        self._last_result = conn.execute(query).fetchall()

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query"""
        if not self._last_result:
            raise RuntimeError("No query has been executed yet")
        return self._last_result[0]
