"""
Write every output table twice: Parquet as the source of truth (keeps dtypes,
safe for free text with newlines/quotes), plus a CSV copy for quick
examination in a spreadsheet or with `head`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

TABLE_SUFFIXES = {".parquet", ".csv"}


def _stem(path: str | Path) -> Path:
    """'data/sample', 'data/sample.parquet' and 'data/sample.csv' all map to
    'data/sample'; any other suffix is kept as part of the name."""
    path = Path(path)
    return path.with_suffix("") if path.suffix in TABLE_SUFFIXES else path


def write_table(df: pd.DataFrame, path: str | Path) -> Path:
    """Write `<stem>.parquet` and `<stem>.csv`; return the Parquet path."""
    stem = _stem(path)
    stem.parent.mkdir(parents=True, exist_ok=True)
    parquet_path = stem.with_name(stem.name + ".parquet")
    csv_path = stem.with_name(stem.name + ".csv")
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
    logger.info(f"Wrote {len(df)} rows to {parquet_path} (+ {csv_path.name})")
    return parquet_path


def read_table(path: str | Path) -> pd.DataFrame:
    """Read a table written by write_table, preferring the Parquet copy."""
    stem = _stem(path)
    parquet_path = stem.with_name(stem.name + ".parquet")
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    return pd.read_csv(
        stem.with_name(stem.name + ".csv"), dtype={"gtl_id": str, "item_id": str}
    )
