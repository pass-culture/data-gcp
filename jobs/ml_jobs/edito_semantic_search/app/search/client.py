import datetime

import lancedb
import polars as pl
from loguru import logger

from app.constants import PARQUET_FILE, embedding_model

DATE_COLS = ["offer_creation_date", "stock_beginning_date"]


def parse_date(val):
    if isinstance(val, datetime.date | datetime.datetime):
        return val
    try:
        return datetime.datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        logger.warning(f"Could not parse date: {val}")
        return val


def apply_filters(lf: pl.LazyFrame, filters: list[dict]) -> pl.LazyFrame:
    for f in filters:
        col = f["column"]
        op = f["operator"].lower()
        val = f["value"]
        if (
            col in DATE_COLS
            and op == "between"
            and isinstance(val, list | tuple)
            and len(val) == 2
        ):
            start, end = parse_date(val[0]), parse_date(val[1])
            lf = lf.filter((pl.col(col) >= start) & (pl.col(col) <= end))
        elif col in DATE_COLS and op != "between":
            val = parse_date(val)
        elif op == "=":
            lf = lf.filter(pl.col(col) == val)
        elif op == "in" and isinstance(val, list | tuple):
            lf = lf.filter(pl.col(col).is_in(val))
        elif op == ">":
            lf = lf.filter(pl.col(col) > val)
        elif op == ">=":
            lf = lf.filter(pl.col(col) >= val)
        elif op == "<":
            lf = lf.filter(pl.col(col) < val)
        elif op == "<=":
            lf = lf.filter(pl.col(col) <= val)
        elif op == "!=" or op == "<>":
            lf = lf.filter(pl.col(col) != val)
        else:
            logger.error(f"Unsupported operator: {op}")
            raise ValueError(f"Unsupported operator: {op}")
    return lf


class SearchClient:
    def __init__(self, database_uri: str, vector_table: str, scalar_table: str):
        """Connects to LanceDB and opens the specified table."""
        self.embedding_model = embedding_model
        logger.info(f"Connecting to LanceDB at: {database_uri}")
        self.db = lancedb.connect(database_uri)
        logger.info(
            f"Opening vector table: {vector_table} and scalar table: {scalar_table}"
        )
        self.vector_table = self.db.open_table(vector_table)

    def table_query(self, k: int = 1000, filters: list[dict] | None = None):
        """
        Performs a scalar search using polars lazy mode on a Parquet directory in GCS
        (supports data-*.parquet).
        """
        lf = pl.scan_parquet(PARQUET_FILE)
        if filters:
            lf_filtered = apply_filters(lf, filters)
        df = lf_filtered.select(["item_id", "offer_id"]).head(k).collect()
        result = df.to_dicts()
        return result

    def vector_search(self, query_vector, k: int = 5):
        """Performs a vector similarity search using LanceDB's search method."""
        return self.vector_table.search(query_vector).limit(k).to_list()
