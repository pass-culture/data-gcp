"""Parquet I/O for the pipeline's GCS-staged steps: streaming reads of a
folder as uniform chunks, a generic writer for the text stages
(preprocessed/prompts), and an explicit-schema writer for embeddings.
"""

import os
from typing import Iterator, Optional

import gcsfs
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from constants import ROWS_PER_CHUNK
from loguru import logger


def list_parquet_files(gcs_path: str) -> list[str]:
    """List all Parquet files matching the given GCS path.

    Raises:
        ValueError: If the path is not a ``gs://`` path.
        FileNotFoundError: If no files are found.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    fs = gcsfs.GCSFileSystem()
    l_files = fs.glob(gcs_path + "/*")
    if not l_files:
        raise FileNotFoundError(f"No files found for path: {gcs_path}")
    return [f"gs://{filename}" for filename in l_files]


def iter_parquet_chunks(
    gcs_path: str,
    rows_per_chunk: int = ROWS_PER_CHUNK,
    required_columns: Optional[list[str]] = None,
) -> Iterator[pd.DataFrame]:
    """Stream every Parquet file under ``gcs_path`` as uniformly-sized chunks.

    All files are treated as one logical dataset, so a yielded chunk may span
    rows from more than one underlying file -- this decouples the chunk size
    from however unevenly BigQuery sharded its export. When ``required_columns``
    is given, it is validated once against the dataset schema before streaming.

    Args:
        gcs_path: Local or ``gs://`` folder path with the input parquet files.
        rows_per_chunk: Target rows per yielded chunk (only the last may be
            smaller).
        required_columns: Columns that must be present, or ``None`` to skip the
            check.

    Yields:
        pd.DataFrame of up to ``rows_per_chunk`` rows, in dataset order.

    Raises:
        FileNotFoundError: If no parquet files are found.
        ValueError: If a required column is missing.
    """
    dataset = ds.dataset(gcs_path, format="parquet")
    if not dataset.files:
        raise FileNotFoundError(f"No files found for path: {gcs_path}")

    if required_columns:
        available = set(dataset.schema.names)
        missing = [c for c in required_columns if c not in available]
        if missing:
            raise ValueError(
                f"Dataset at {gcs_path} is missing required columns: "
                f"{', '.join(missing)}"
            )

    logger.info(
        f"Streaming {gcs_path} ({len(dataset.files)} file(s)) "
        f"in chunks of up to {rows_per_chunk} rows"
    )

    # pyarrow caps each raw batch at rows_per_chunk but never merges across
    # file/fragment boundaries, so a small input file always produces its own
    # short trailing batch. Buffer raw batches and re-slice into uniform
    # rows_per_chunk pieces, so a yielded chunk can span multiple files. The
    # buffer never holds more than ~2x rows_per_chunk rows.
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


def _write_table(table: pa.Table, gcs_path: str) -> None:
    if gcs_path.startswith("gs://"):
        fs = gcsfs.GCSFileSystem()
        with fs.open(gcs_path, "wb") as f:
            pq.write_table(table, f, compression="snappy")
    else:
        os.makedirs(os.path.dirname(gcs_path) or ".", exist_ok=True)
        pq.write_table(table, gcs_path, compression="snappy")


def write_parquet(df: pd.DataFrame, gcs_path: str) -> None:
    """Write a DataFrame to parquet (schema inferred). Used for the text stages
    (preprocessed features, prompts), whose columns are plain strings.
    """
    _write_table(pa.Table.from_pandas(df, preserve_index=False), gcs_path)


def _embeddings_schema() -> pa.Schema:
    """Explicit Arrow schema for an embeddings chunk: string ids plus an
    ``embedding`` ``list<float>`` field.
    """
    return pa.schema(
        [
            pa.field("item_id", pa.string()),
            pa.field("content_hash", pa.string()),
            pa.field("embedding", pa.list_(pa.field("element", pa.float32()))),
        ]
    )


def write_embeddings_parquet(df: pd.DataFrame, gcs_path: str) -> None:
    """Write an embeddings chunk with an explicit, uniform schema so every chunk
    agrees on the ``embedding`` column's Arrow type (``list<float>``). Without
    it, an empty chunk would infer Arrow's ``null`` type and break BigQuery's
    autodetect unification into a single REPEATED FLOAT column across files.

    Args:
        df: item_id, content_hash, embedding (list[float] per row).
        gcs_path: Destination parquet path (local or ``gs://...``).
    """
    table = pa.Table.from_pandas(df, schema=_embeddings_schema(), preserve_index=False)
    _write_table(table, gcs_path)
