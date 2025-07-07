import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import psycopg2
from google.cloud import bigquery
from psycopg2.extensions import connection

from utils.constant import DUCKDB_DATABASE_EXTENSIONS, MAX_RETRIES

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception class for database-related errors."""

    pass


class NoQueryExecutedError(DatabaseError):
    """Raised when trying to fetch results before executing a query."""

    def __init__(self):
        super().__init__("No query has been executed yet")


class DatabaseService(ABC):
    """Abstract base class for database operations."""

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a database query."""
        pass

    @abstractmethod
    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass


class BigQueryService(DatabaseService):
    """Service for interacting with Google BigQuery."""

    def __init__(self, project_id: str):
        self.client = bigquery.Client(project=project_id)
        self._last_query_result = None

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a BigQuery query."""
        try:
            if params and "destination" in params:
                job_config = bigquery.QueryJobConfig(
                    destination=params["destination"],
                    write_disposition=params.get("write_disposition", "WRITE_TRUNCATE"),
                )
                query_job = self.client.query(query, job_config=job_config)
            else:
                query_job = self.client.query(query)

            self._last_query_result = query_job.result()
        except Exception as e:
            logger.error(f"Error executing BigQuery query: {str(e)}")
            raise DatabaseError(f"Failed to execute query: {str(e)}")

    def export_data(
        self, source_ref: str, destination_uri: str, format: str = "PARQUET"
    ) -> None:
        """Export BigQuery table to GCS."""
        try:
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
        except Exception as e:
            logger.error(f"Error exporting data from BigQuery: {str(e)}")
            raise DatabaseError(f"Failed to export data: {str(e)}")

    def load_from_gcs(
        self,
        table_id: str,
        gcs_path: str,
        schema: Optional[List[bigquery.SchemaField]] = None,
        write_disposition: str = "WRITE_APPEND",
        time_partitioning: Optional[bigquery.TimePartitioning] = None,
    ) -> None:
        """Load data from GCS Parquet files into a BigQuery table."""
        try:
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
            logger.info(f"Loaded data into {table_id}")
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {str(e)}")
            raise DatabaseError(f"Failed to load data: {str(e)}")

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query."""
        if not self._last_query_result:
            raise NoQueryExecutedError()
        return next(iter(self._last_query_result))

    def close(self) -> None:
        """Close the BigQuery client."""
        self.client.close()


class CloudSQLService(DatabaseService):
    """Service for interacting with Cloud SQL."""

    def __init__(self, connection_params: Dict):
        self.connection_params = connection_params
        self._connection = None
        self._cursor = None
        self._last_result = None

    def _get_connection(self) -> connection:
        """Get a database connection with retries."""
        retry_count = 0
        database_url = self.connection_params["database_url"]
        while retry_count < MAX_RETRIES:
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
                    raise DatabaseError(f"Failed to connect to database: {str(e)}")

                wait_time = min(30, 5 * retry_count)
                logger.warning(
                    f"Database connection failed (attempt {retry_count}/{MAX_RETRIES}). Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)

    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
    ) -> None:
        """Execute a query on CloudSQL."""
        try:
            if not self._connection or self._connection.closed:
                self._connection = self._get_connection()
                self._cursor = self._connection.cursor()

            self._cursor.execute(query, params or {})
            if self._cursor.description:  # Query returns rows
                self._last_result = self._cursor.fetchall()
            else:  # Query does not return rows (DDL, INSERT, UPDATE, DELETE)
                self._last_result = None
            self._connection.commit()
        except Exception as e:
            logger.error(f"Error executing CloudSQL query: {str(e)}")
            raise DatabaseError(f"Failed to execute query: {str(e)}")

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query."""
        if not self._last_result:
            raise NoQueryExecutedError()
        return self._last_result[0]

    def fetch_all(self) -> List[Tuple[Any, ...]]:
        """Fetch all rows from the last executed query."""
        if not self._last_result:
            raise NoQueryExecutedError()
        return self._last_result

    def close(self) -> None:
        """Close the database connection and cursor."""
        if self._cursor:
            self._cursor.close()
        if self._connection:
            self._connection.close()
        self._last_result = None


class DuckDBService(DatabaseService):
    """Service for interacting with DuckDB."""

    def __init__(
        self,
        database_path: str = ":memory:",
        extensions: List[str] = DUCKDB_DATABASE_EXTENSIONS,
    ):
        self.database_path = database_path
        self.extensions = extensions
        self._last_result = None
        self._connection = None

    def setup_connection(self) -> duckdb.DuckDBPyConnection:
        """Setup a new DuckDB connection with required extensions."""
        if self._connection is None:
            try:
                self._connection = duckdb.connect(self.database_path)
                for extension in self.extensions:
                    self._connection.execute(f"INSTALL {extension}; LOAD {extension};")
            except Exception as e:
                logger.error(f"Failed to setup DuckDB connection: {str(e)}")
                raise DatabaseError(f"Failed to setup connection: {str(e)}")
        return self._connection

    def execute_query(self, query: str, params: Optional[Dict] = None) -> None:
        """Execute a query using the maintained connection."""
        try:
            conn = self.setup_connection()
            self._last_result = conn.execute(query).fetchall()
        except Exception as e:
            logger.error(f"Error executing DuckDB query: {str(e)}")
            raise DatabaseError(f"Failed to execute query: {str(e)}")

    def fetch_one(self) -> Tuple[Any, ...]:
        """Fetch one row from the last executed query."""
        if not self._last_result:
            raise NoQueryExecutedError()
        return self._last_result[0]

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
