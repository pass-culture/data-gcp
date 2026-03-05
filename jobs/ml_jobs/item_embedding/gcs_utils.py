import os

import gcsfs
import pyarrow.parquet as pq
from loguru import logger


def list_parquet_files(gcs_path: str) -> list[str]:
    """List all Parquet files matching the given path.
    Args:
        gcs_path: GCS path
    returns:
        List of GCS paths to Parquet files
    raises:
        ValueError: If the input path is not a valid GCS path.
        FileNotFoundError: If no files are found matching the path.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    fs = gcsfs.GCSFileSystem()
    files = fs.glob(gcs_path)
    if not files:
        raise FileNotFoundError(f"No files found for path: {gcs_path}")
    return files


def load_parquet_file(
    parquet_filename: str,
    required_columns: list[str],
) -> pq.ParquetFile:
    """Load a Parquet file.

    Accepts a single parquet filename, returns a ParquetFile object that can be read in batches.
    Supports local or GCS paths.

    Args:
        parquet_filename: Local or GCS (``gs://…``) path — file, directory, or glob.

    Returns:
        pq.ParquetFile

    Raises:
        FileNotFoundError: If the path does not exist or matches no files.
    """
    logger.info(f"Loading data from: {parquet_filename}")

    pf = pq.ParquetFile(parquet_filename)

    if required_columns is not None:
        _validate_parquet_file(pf, required_columns)

    logger.info(f"Loaded {pf.metadata.num_rows} items from {parquet_filename}")
    return pf


def _validate_parquet_file(pf: pq.ParquetFile, required_columns: list[str]) -> None:
    """Validate that a Parquet file contains the required columns.

    Args:
        pf: ParquetFile object.
        required_columns: List of column names that must be present in the Parquet schema.

    Raises:
        ValueError: If any required column is missing from the Parquet schema.
    """
    available = set(pf.schema.names)
    missing = [c for c in required_columns if c not in available]
    if missing:
        raise ValueError(
            f"Parquet file is missing required columns: {', '.join(missing)}"
        )


def upload_file_to_gcs(local_path: str, gcs_path: str) -> None:
    """Upload a local file to GCS and remove the local copy.

    Args:
        local_path: Absolute path to the local file.
        gcs_path: Destination GCS path (``gs://bucket/path/file.parquet``).

    Raises:
        FileNotFoundError: If the local file does not exist.
        ValueError: If *gcs_path* is not a valid ``gs://`` URI.
    """
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    logger.info(f"Uploading {local_path} -> {gcs_path}")
    fs = gcsfs.GCSFileSystem()
    fs.put(local_path, gcs_path)
    logger.info(f"Upload complete: {gcs_path}")

    os.remove(local_path)
    logger.info(f"Removed local temp file: {local_path}")
