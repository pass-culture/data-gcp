from typing import Iterator

import gcsfs
import pandas as pd
import pyarrow.dataset as ds
from constants import ROWS_PER_CHUNK
from loguru import logger


def list_parquet_files(gcs_path: str) -> list[str]:
    """List all Parquet files matching the given path.
    Args:
        gcs_path: GCS path
    Returns:
        List of GCS paths to Parquet files starting with gs://
    Raises:
        ValueError: If the input path is not a valid GCS path.
        FileNotFoundError: If no files are found matching the path.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    fs = gcsfs.GCSFileSystem()
    l_files = fs.glob(gcs_path + "/*")
    if not l_files:
        raise FileNotFoundError(f"No files found for path: {gcs_path}")
    return [f"gs://{filename}" for filename in l_files]


def iter_metadata_chunks(
    gcs_path: str,
    vectors: list,
    rows_per_chunk: int = ROWS_PER_CHUNK,
) -> Iterator[pd.DataFrame]:
    """Stream item metadata as uniformly-sized DataFrame chunks.

    Every Parquet file under ``gcs_path`` is treated as one logical dataset
    (via ``pyarrow.dataset``), so a yielded chunk may span rows from more
    than one underlying file -- this decouples the embedding batch size from
    however unevenly BigQuery's wildcard export happened to shard the input
    (some files can be much larger than others). Required columns are
    validated once against the dataset schema before any data is streamed.

    Args:
        gcs_path: Local or GCS (``gs://…``) folder path containing the
            input parquet files.
        vectors: List of vector configurations used to determine required
            columns.
        rows_per_chunk: Target number of rows per yielded chunk. A chunk is
            never larger than this; only the last chunk of the whole
            dataset may be smaller.

    Yields:
        pd.DataFrame of up to ``rows_per_chunk`` rows, in dataset order.

    Raises:
        FileNotFoundError: If no parquet files are found at ``gcs_path``.
        ValueError: If required columns are missing from the dataset schema.
    """
    dataset = ds.dataset(gcs_path, format="parquet")
    if not dataset.files:
        raise FileNotFoundError(f"No files found for path: {gcs_path}")

    available = set(dataset.schema.names)
    missing = [c for c in _required_columns(vectors) if c not in available]
    if missing:
        raise ValueError(
            f"Input dataset at {gcs_path} is missing required columns: "
            f"{', '.join(missing)}"
        )

    logger.info(
        f"Streaming {gcs_path} ({len(dataset.files)} file(s)) "
        f"in chunks of up to {rows_per_chunk} rows"
    )

    # pyarrow caps each raw batch at rows_per_chunk but never merges across
    # file/fragment boundaries, so a small input file always produces its
    # own short trailing batch on its own (verified empirically). Buffer
    # raw batches ourselves and re-slice into uniform rows_per_chunk
    # pieces, so a yielded chunk can transparently span multiple input
    # files -- this is what actually fixes small-file GPU underutilization.
    # The buffer never holds more than ~2x rows_per_chunk rows, so this
    # stays memory-safe for a full-catalogue run.
    pending: list[pd.DataFrame] = []
    pending_rows = 0
    for batch in dataset.to_batches(batch_size=rows_per_chunk):
        pending.append(batch.to_pandas())
        pending_rows += batch.num_rows
        if pending_rows < rows_per_chunk:
            continue

        buffered = pd.concat(pending, ignore_index=True)
        while len(buffered) >= rows_per_chunk:
            yield buffered.iloc[:rows_per_chunk].reset_index(drop=True)
            buffered = buffered.iloc[rows_per_chunk:].reset_index(drop=True)
        pending = [buffered] if len(buffered) else []
        pending_rows = len(buffered)

    if pending:
        remainder = pd.concat(pending, ignore_index=True)
        if len(remainder) > 0:
            yield remainder


def _required_columns(vectors: list) -> list[str]:
    """Returns the deduplicated list of columns required to embed ``vectors``:
    ``item_id``, ``content_hash``, and every feature referenced by any vector.
    """
    return list(
        set(
            ["item_id", "content_hash"]
            + [feature for vector in vectors for feature in vector.features]
        )
    )


def _validate_parquet_file(df: pd.DataFrame, vectors: list) -> None:
    """Validate that a DataFrame contains all required columns.

    Args:
        df: DataFrame to validate.
        vectors: List of vector configurations.

    Raises:
        ValueError: If any required column is missing from the DataFrame.
    """
    available = set(df.columns)
    missing = [c for c in _required_columns(vectors) if c not in available]
    if missing:
        raise ValueError(f"DataFrame is missing required columns: {', '.join(missing)}")
