import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb
from google.cloud import bigquery, storage

from utils import (
    BIGQUERY_RAW_DATASET,
    PROJECT_NAME,
    RECOMMENDATION_SQL_INSTANCE,
    access_secret_data,
    get_db_connection,
)

logger = logging.getLogger(__name__)


class PastOfferContextConfig:
    """Configuration for past_offer_context table."""

    name = "past_offer_context"
    time_column = "date"
    partition_field = "import_date"
    hour_field = "import_hour"

    # Define all columns with their data types
    columns = {
        "id": "bigint",
        "call_id": "character varying",
        "context": "jsonb",
        "context_extra_data": "jsonb",
        "date": "timestamp without time zone",
        "user_id": "character varying",
        "user_bookings_count": "integer",
        "user_clicks_count": "integer",
        "user_favorites_count": "integer",
        "user_deposit_remaining_credit": "numeric",
        "user_iris_id": "character varying",
        "user_is_geolocated": "boolean",
        "user_extra_data": "jsonb",
        "offer_user_distance": "numeric",
        "offer_is_geolocated": "boolean",
        "offer_id": "character varying",
        "offer_item_id": "character varying",
        "offer_booking_number": "integer",
        "offer_stock_price": "numeric",
        "offer_creation_date": "timestamp without time zone",
        "offer_stock_beginning_date": "timestamp without time zone",
        "offer_category": "character varying",
        "offer_subcategory_id": "character varying",
        "offer_item_rank": "integer",
        "offer_item_score": "numeric",
        "offer_order": "integer",
        "offer_venue_id": "character varying",
        "offer_extra_data": "jsonb",
    }

    @classmethod
    def get_incremental_query(cls, hour: int, execution_date: datetime) -> str:
        """Generate query to extract incremental data for the specified hour."""
        date_str = execution_date.strftime("%Y-%m-%d")
        hour_start = f"{date_str} {hour:02d}:00:00"
        hour_end = f"{date_str} {hour:02d}:59:59"

        # Get column list with special handling for any geometry types
        columns_str = ", ".join(cls.columns.keys())

        return f"""
            SELECT
                {columns_str},
                '{date_str}' as {cls.partition_field},
                {hour} as {cls.hour_field}
            FROM past_offer_context
            WHERE {cls.time_column} >= '{hour_start}'
              AND {cls.time_column} <= '{hour_end}'
        """

    @classmethod
    def get_recovery_query(cls, start_time: datetime, end_time: datetime) -> str:
        """
        Generate query to extract data between two timestamps for recovery.

        Args:
            start_time: Start timestamp for recovery
            end_time: End timestamp for recovery

        Returns:
            SQL query string for recovery
        """
        # Get column list
        columns_str = ", ".join(cls.columns.keys())

        return f"""
            SELECT
                {columns_str},
                DATE({cls.time_column}) as {cls.partition_field},
                EXTRACT(HOUR FROM {cls.time_column}) as {cls.hour_field}
            FROM past_offer_context
            WHERE {cls.time_column} >= '{start_time.isoformat()}'
              AND {cls.time_column} <= '{end_time.isoformat()}'
        """


def get_last_successful_export_time(bucket_path: str, table_name: str) -> datetime:
    """
    Find the timestamp of the last successful export by checking BigQuery.

    Args:
        bucket_path: GCS bucket path (not used but kept for API compatibility)
        table_name: Name of the table

    Returns:
        Datetime of the last successful export or a default time (24h ago)
    """
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Query to find the latest import_date and import_hour in BigQuery
        query = f"""
        WITH latest_partitions AS (
            SELECT
                table_name,
                REPLACE(table_name, '{table_name}$', '') AS partition_id
            FROM
                `{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.__TABLES__`
            WHERE
                table_name LIKE '{table_name}$%'
        ),
        latest_hourly_data AS (
            SELECT
                PARSE_DATE('%Y%m%d', partition_id) AS partition_date,
                MAX(import_hour) AS max_hour
            FROM
                latest_partitions lp,
                `{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.{table_name}$*` t
            WHERE
                CONCAT('{table_name}$', REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+')) = lp.table_name
            GROUP BY
                partition_date
            ORDER BY
                partition_date DESC, max_hour DESC
            LIMIT 1
        )
        SELECT
            partition_date, max_hour
        FROM
            latest_hourly_data
        """

        # Execute the query
        logger.info("Querying BigQuery for latest import timestamp")
        query_job = client.query(query)
        result = list(query_job.result())

        if not result:
            # If no data found in BigQuery, default to 24 hours ago
            logger.info(
                "No previous imports found in BigQuery, defaulting to 24 hours ago"
            )
            return datetime.now() - timedelta(hours=24)

        # Parse the result
        last_date = result[0].partition_date
        last_hour = result[0].max_hour

        # Create a datetime from the partition date and hour
        last_import_time = datetime.combine(
            last_date,
            datetime.min.time(),
        ).replace(hour=last_hour)

        logger.info(f"Found last import in BigQuery at {last_import_time}")

        # Add 1 hour to start from the next hour
        return last_import_time + timedelta(hours=1)

    except Exception as e:
        logger.warning(
            f"Error finding last successful import in BigQuery: {str(e)}, defaulting to 24 hours ago"
        )
        return datetime.now() - timedelta(hours=24)


def export_hourly_data_to_gcs(
    table_name: str,
    bucket_path: str,
    execution_date: datetime,
    hour: int,
    recover_missed: bool = False,
) -> Optional[List[str]]:
    """
    Export incremental data from CloudSQL to GCS using DuckDB.

    Args:
        table_name: Name of the table to export
        bucket_path: GCS bucket path
        execution_date: Execution date
        hour: Hour of the day to export
        recover_missed: If True, checks for and recovers missed data since last successful export

    Returns:
        List of GCS file paths if data was exported, None otherwise
    """
    start_time = time.time()

    # Currently we only support past_offer_context
    if table_name != "past_offer_context":
        raise ValueError(f"Table {table_name} is not supported for incremental export")

    config = PastOfferContextConfig
    date_nodash = execution_date.strftime("%Y%m%d")

    # Check if we need to recover missed data
    export_time_ranges = []
    if recover_missed:
        # Get last successful export time
        last_export_time = get_last_successful_export_time(bucket_path, table_name)
        current_time = execution_date.replace(
            hour=hour, minute=0, second=0, microsecond=0
        )

        # If there's a gap, prepare for recovery
        if last_export_time < current_time:
            logger.info(
                f"Detected gap in exports from {last_export_time} to {current_time}, recovering missed data"
            )

            # Generate hourly time ranges for recovery
            current = last_export_time
            while current < current_time:
                end_time = current + timedelta(hours=1) - timedelta(seconds=1)
                export_time_ranges.append((current, end_time))
                current += timedelta(hours=1)
        else:
            # No gap, just export the current hour
            hour_start = execution_date.replace(
                hour=hour, minute=0, second=0, microsecond=0
            )
            hour_end = hour_start + timedelta(hours=1) - timedelta(seconds=1)
            export_time_ranges.append((hour_start, hour_end))
    else:
        # Regular hourly export without recovery
        hour_start = execution_date.replace(
            hour=hour, minute=0, second=0, microsecond=0
        )
        hour_end = hour_start + timedelta(hours=1) - timedelta(seconds=1)
        export_time_ranges.append((hour_start, hour_end))

    if not export_time_ranges:
        logger.info("No time ranges to export")
        return None

    logger.info(f"Will export data for {len(export_time_ranges)} time ranges")

    # Create temporary directory for intermediate storage
    temp_dir = Path(f"/tmp/cloudsql_export/{date_nodash}/{hour}")
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Get database connection details
    database_url = access_secret_data(
        PROJECT_NAME,
        f"{RECOMMENDATION_SQL_INSTANCE}_database_url",
    )

    exported_paths = []

    # Process each time range
    for start_time, end_time in export_time_ranges:
        range_hour = start_time.hour
        range_date_str = start_time.strftime("%Y-%m-%d")
        range_date_nodash = start_time.strftime("%Y%m%d")

        logger.info(f"Exporting data for time range: {start_time} to {end_time}")

        # Local file path for this time range
        local_file_path = (
            temp_dir / f"{table_name}_{range_date_nodash}_{range_hour:02d}.parquet"
        )

        # Initialize DuckDB connection
        duck_conn = duckdb.connect(memory=True)

        try:
            # Install and load required extensions
            logger.info("Installing and loading required DuckDB extensions")
            duck_conn.execute("INSTALL postgres")
            duck_conn.execute("LOAD postgres")
            duck_conn.execute("INSTALL httpfs")
            duck_conn.execute("LOAD httpfs")

            # Set GCS credentials to use VM's service account
            duck_conn.execute("SET s3_region='auto'")
            duck_conn.execute("SET credentials_json=gcloud")

            # Attach PostgreSQL database
            logger.info("Connecting to PostgreSQL database")
            duck_conn.execute(f"ATTACH '{database_url}' AS pg_db (TYPE postgres)")

            # Generate query for this time range
            if start_time.date() == end_time.date() and not recover_missed:
                # Use hourly query for same-day exports without recovery
                incremental_query = config.get_incremental_query(range_hour, start_time)
            else:
                # Use recovery query for cross-day exports or recovery
                incremental_query = config.get_recovery_query(start_time, end_time)

            # Extract data from PostgreSQL and save to local Parquet file
            logger.info(
                f"Extracting data for {table_name} from {start_time} to {end_time}"
            )
            duck_conn.execute(f"""
                COPY (
                    SELECT * FROM pg_db.({incremental_query})
                ) TO '{local_file_path}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)

            # Check if file was created and has data
            if (
                not os.path.exists(local_file_path)
                or os.path.getsize(local_file_path) == 0
            ):
                logger.info(
                    f"No data found for {table_name} between {start_time} and {end_time}"
                )
                continue

            # Get row count
            row_count = duck_conn.execute(f"""
                SELECT COUNT(*) FROM '{local_file_path}'
            """).fetchone()[0]

            logger.info(
                f"Extracted {row_count} rows for {table_name} from {start_time} to {end_time}"
            )

            # Upload directly to GCS
            gcs_file_path = f"{bucket_path}/{table_name}/{range_date_str}/hour_{range_hour:02d}.parquet"

            logger.info(f"Uploading data to GCS: {gcs_file_path}")

            # Upload using DuckDB httpfs
            duck_conn.execute(f"""
                COPY (
                    SELECT * FROM '{local_file_path}'
                ) TO '{gcs_file_path}'
                (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)

            logger.info(
                f"Successfully uploaded {row_count} rows to GCS: {gcs_file_path}"
            )
            exported_paths.append(gcs_file_path)

        except Exception as e:
            logger.error(
                f"Error processing time range {start_time} to {end_time}: {str(e)}"
            )
            # Continue with next time range
        finally:
            # Clean up
            duck_conn.close()

            # Remove temporary file
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

    # Try to remove directory if empty
    try:
        if len(os.listdir(temp_dir)) == 0:
            os.rmdir(temp_dir)
    except (OSError, IOError) as e:
        logger.warning(f"Could not remove temporary directory: {str(e)}")

    elapsed_time = time.time() - start_time
    if exported_paths:
        logger.info(
            f"Completed export of {table_name} for {len(exported_paths)} time ranges in {elapsed_time:.2f} seconds"
        )
        return exported_paths
    else:
        logger.info(
            f"No data exported for any time range after {elapsed_time:.2f} seconds"
        )
        return None


def load_gcs_to_bigquery(
    table_name: str,
    bucket_path: str,
    execution_date: datetime,
    hour: int,
    gcs_paths: Optional[List[str]] = None,
) -> None:
    """
    Load exported data from GCS to BigQuery.

    Args:
        table_name: Name of the table
        bucket_path: GCS bucket path where data was exported
        execution_date: Execution date
        hour: Hour of the export
        gcs_paths: Optional list of specific GCS paths to load
    """
    if table_name != "past_offer_context":
        raise ValueError(f"Table {table_name} is not supported for incremental load")

    # If specific paths are provided, use them
    # Otherwise, construct the path for the current hour
    if not gcs_paths:
        date_str = execution_date.strftime("%Y-%m-%d")
        date_nodash = execution_date.strftime("%Y%m%d")
        gcs_paths = [f"{bucket_path}/{table_name}/{date_str}/hour_{hour:02d}.parquet"]

    # Initialize BigQuery client
    client = bigquery.Client()

    # Process each path
    for gcs_file_path in gcs_paths:
        logger.info(f"Loading data from GCS to BigQuery: {gcs_file_path}")

        # Extract date from path for partitioning
        try:
            # Parse path to get date for the partition
            path_parts = gcs_file_path.split("/")
            date_str = path_parts[-2]  # Format: YYYY-MM-DD
            date_nodash = date_str.replace("-", "")

            # Define the table reference with daily partitioning
            table_ref = (
                f"{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.{table_name}${date_nodash}"
            )
        except (IndexError, ValueError):
            logger.error(f"Failed to parse date from path: {gcs_file_path}")
            continue

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Check if the file exists in GCS
        storage_client = storage.Client()
        parsed_url = gcs_file_path.replace("gs://", "").split("/", 1)
        bucket_name = parsed_url[0]
        blob_name = parsed_url[1]

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            logger.warning(
                f"No data file found at {gcs_file_path}, skipping BigQuery load"
            )
            continue

        # Load data to BigQuery
        try:
            load_job = client.load_table_from_uri(
                gcs_file_path,
                table_ref,
                job_config=job_config,
            )

            # Wait for the job to complete
            load_job.result()

            logger.info(f"Successfully loaded data from {gcs_file_path} to {table_ref}")

        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {str(e)}")
            # Continue with next file


def delete_processed_data_from_cloudsql(
    table_name: str,
    execution_date: datetime,
    hour: int,
    time_ranges: Optional[List[Tuple[datetime, datetime]]] = None,
) -> None:
    """
    Delete processed data from CloudSQL to prevent duplicate exports.

    Args:
        table_name: Name of the table
        execution_date: Execution date
        hour: Hour of the export
        time_ranges: Optional list of (start_time, end_time) tuples for more precise deletion
    """
    if table_name != "past_offer_context":
        raise ValueError(f"Table {table_name} is not supported for deletion")

    # Get database connection
    conn = get_db_connection()

    try:
        # If specific time ranges are provided, use them
        if time_ranges:
            for start_time, end_time in time_ranges:
                delete_for_time_range(conn, table_name, start_time, end_time)
        else:
            # Regular hourly deletion
            date_str = execution_date.strftime("%Y-%m-%d")
            hour_start = f"{date_str} {hour:02d}:00:00"
            hour_end = f"{date_str} {hour:02d}:59:59"
            delete_for_time_range(conn, table_name, hour_start, hour_end)
    finally:
        conn.close()


def delete_for_time_range(conn, table_name: str, start_time, end_time) -> None:
    """Helper function to delete data for a specific time range."""
    try:
        with conn.cursor() as cursor:
            # Check how many rows will be deleted
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM public.{table_name}
                WHERE date >= %s AND date <= %s
                """,
                (start_time, end_time),
            )
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                logger.info(
                    f"No data to delete for {table_name} between {start_time} and {end_time}"
                )
                return

            # Delete the data
            cursor.execute(
                f"""
                DELETE FROM public.{table_name}
                WHERE date >= %s AND date <= %s
                """,
                (start_time, end_time),
            )

            conn.commit()

            logger.info(
                f"Successfully deleted {row_count} rows from {table_name} between {start_time} and {end_time}"
            )

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to delete data from CloudSQL: {str(e)}")
        raise


def full_export_process(
    table_name: str,
    bucket_path: str,
    execution_date: datetime,
    hour: int,
    recover_missed: bool = True,
) -> None:
    """
    Run the full export process: CloudSQL -> GCS -> BigQuery.

    Args:
        table_name: Name of the table to process
        bucket_path: GCS bucket path for storage
        execution_date: Execution date
        hour: Hour of the export
        recover_missed: Whether to recover missed data from previous failed jobs
    """
    logger.info(
        f"Starting full export process for {table_name} at {execution_date.strftime('%Y-%m-%d')} hour {hour}"
    )
    logger.info(f"Recovery mode: {recover_missed}")

    # Step 1: Export data from CloudSQL to GCS
    gcs_paths = export_hourly_data_to_gcs(
        table_name=table_name,
        bucket_path=bucket_path,
        execution_date=execution_date,
        hour=hour,
        recover_missed=recover_missed,
    )

    # If no data was exported, skip the next steps
    if not gcs_paths:
        logger.info(
            f"No data exported for {table_name} at hour {hour}, skipping remaining steps"
        )
        return

    # Step 2: Load data from GCS to BigQuery
    load_gcs_to_bigquery(
        table_name=table_name,
        bucket_path=bucket_path,
        execution_date=execution_date,
        hour=hour,
        gcs_paths=gcs_paths,
    )

    if recover_missed and len(gcs_paths) > 1:
        time_ranges = []
        for path in gcs_paths:
            try:
                path_parts = path.split("/")
                date_str = path_parts[-2]  # Format: YYYY-MM-DD
                hour_part = path_parts[-1]  # Format: hour_HH.parquet
                hour_val = int(hour_part.replace("hour_", "").replace(".parquet", ""))

                start_time = datetime.strptime(
                    f"{date_str} {hour_val:02d}:00:00", "%Y-%m-%d %H:%M:%S"
                )
                end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)

                time_ranges.append((start_time, end_time))
            except (ValueError, IndexError):
                logger.warning(f"Could not parse time range from path: {path}")

        if time_ranges:
            delete_processed_data_from_cloudsql(
                table_name=table_name,
                execution_date=execution_date,
                hour=hour,
                time_ranges=time_ranges,
            )
    else:
        # Regular hourly deletion
        delete_processed_data_from_cloudsql(
            table_name=table_name,
            execution_date=execution_date,
            hour=hour,
        )

    logger.info(f"Completed full export process for {table_name} at hour {hour}")
