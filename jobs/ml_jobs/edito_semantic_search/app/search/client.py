import datetime
from itertools import product

import lancedb
import pyarrow.dataset as ds
from pyarrow.fs import GcsFileSystem
import polars as pl
from loguru import logger

from app.constants import PARQUET_FILE, embedding_model

DATE_COLS = ["offer_creation_date", "stock_beginning_date"]
PARTITION_COLS = ["offer_subcategory_id", "venue_department_code"]


def parse_date(val):
    if isinstance(val, datetime.date | datetime.datetime):
        return val
    try:
        return datetime.datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        logger.warning(f"Could not parse date: {val}")
        return val


def _cast_value(val, target_dtype, col):
    """Cast a filter value (or list of values) to match the Polars column dtype."""
    if col in DATE_COLS:
        if isinstance(val, (list, tuple)):
            return [parse_date(v) for v in val]
        return parse_date(val)

    type_map = {
        **dict.fromkeys([pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64], int),
        **dict.fromkeys([pl.Float32, pl.Float64], float),
        **dict.fromkeys([pl.String, pl.Utf8], str),
    }

    cast_fn = type_map.get(target_dtype)
    if cast_fn is None:
        return val

    try:
        if isinstance(val, (list, tuple)):
            return [cast_fn(v) for v in val]
        return cast_fn(val)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot cast '{val}' to {cast_fn.__name__} for column '{col}'") from e


OPERATOR_MAP = {
    "=": lambda c, v: c == v,
    "!=": lambda c, v: c != v,
    "<>": lambda c, v: c != v,
    ">": lambda c, v: c > v,
    ">=": lambda c, v: c >= v,
    "<": lambda c, v: c < v,
    "<=": lambda c, v: c <= v,
    "=<": lambda c, v: c <= v,
    "in": lambda c, v: c.is_in(v),
    "not in": lambda c, v: ~c.is_in(v),
}


def apply_filters(lf: pl.LazyFrame, filters: list[dict]) -> pl.LazyFrame:
    schema = lf.collect_schema()

    for f in filters:
        col, op, val = f["column"], f["operator"].lower(), f["value"]

        if col not in schema:
            logger.warning(f"Column '{col}' not found in dataset — skipping.")
            continue

        val = _cast_value(val, schema[col], col)

        # Special case: date "between"
        if col in DATE_COLS and op == "between" and isinstance(val, (list, tuple)) and len(val) == 2:
            lf = lf.filter((pl.col(col) >= val[0]) & (pl.col(col) <= val[1]))
            continue

        filter_fn = OPERATOR_MAP.get(op)
        if filter_fn is None:
            raise ValueError(f"Unsupported operator: '{op}'")

        lf = lf.filter(filter_fn(pl.col(col), val))

    return lf


class SearchClient:
    def __init__(self, database_uri: str, vector_table: str, scalar_table: str):
        """Connects to LanceDB and opens the specified table."""
        self.embedding_model = embedding_model
        logger.info(f"Connecting to LanceDB at: {database_uri}")
        self.db = lancedb.connect(database_uri)

        logger.info(f"Opening vector table: {vector_table}")
        self.vector_table = self.db.open_table(vector_table)

        # Cache the global dataset once at startup
        logger.info("Initializing global PyArrow Dataset for fallback queries...")
        gcs = GcsFileSystem()
        gcs_path = PARQUET_FILE.replace("gs://", "")
        self.dataset = ds.dataset(
            gcs_path,
            filesystem=gcs,
            format="parquet",
            partitioning="hive",
        )
        self.global_lf = pl.scan_pyarrow_dataset(self.dataset)

    # ── Partition helpers ────────────────────────────────────────────────

    @staticmethod
    def _extract_partition_values(
        filters: list[dict], column: str
    ) -> list[str] | None:
        """
        Collect all values for a partition column across filters.
        Supports '=' (single) and 'in' (multi-value) operators.
        Returns None when the column is absent from filters.
        """
        values: list[str] = []
        for f in filters:
            if f["column"] != column:
                continue
            op = f["operator"].lower()
            if op == "=":
                values.append(str(f["value"]))
            elif op == "in" and isinstance(f["value"], (list, tuple)):
                values.extend(str(v) for v in f["value"])
        return values or None

    def _resolve_lazyframe(self, filters: list[dict] | None) -> pl.LazyFrame:
        """
        Build the narrowest possible LazyFrame based on partition filters.

        Priority:
          1. Both partition cols → scan exact paths (cartesian product)
          2. Only offer_subcategory_id → scan subcategory directories (all dept codes)
          3. Only venue_department_code → scan dept code directories (all subcategories)
          4. Neither → fall back to cached global PyArrow LazyFrame

        Each level avoids reading files from irrelevant partitions on GCS.
        """
        if not filters:
            return self.global_lf

        sub_ids = self._extract_partition_values(filters, "offer_subcategory_id")
        dep_codes = self._extract_partition_values(filters, "venue_department_code")
        # logger.info(f"Extracted partition filters - offer_subcategory_id: {sub_ids}, venue_department_code: {dep_codes}")
        # No partition filters at all → global fallback
        if not sub_ids and not dep_codes:
            logger.debug("No partition filters — using global LazyFrame.")
            return self.global_lf

        # Build the most specific glob paths possible
        if sub_ids and dep_codes:
            # Both partitions → exact directory paths
            paths = [
                f"{PARQUET_FILE}/offer_subcategory_id={sid}/venue_department_code={dc}/*.parquet"
                for sid, dc in product(sub_ids, dep_codes)
            ]
            # logger.info(f"Both partitions → exact directory paths: {paths}")
        elif sub_ids:
            # Only subcategory → wildcard on dept code
            paths = [
                f"{PARQUET_FILE}/offer_subcategory_id={sid}/**/*.parquet"
                for sid in sub_ids
            ]
            # logger.info(f"Only subcategory → wildcard on dept code: {paths}")
        else:
            # Only dept code → wildcard on subcategory
            paths = [
                f"{PARQUET_FILE}/offer_subcategory_id=*/venue_department_code={dc}/*.parquet"
                for dc in dep_codes
            ]
            # logger.info(f"Only dept code → wildcard on subcategory: {paths}")

        logger.debug(f"Fast path: scanning {len(paths)} partition glob(s)")

        try:
            return pl.scan_parquet(paths, hive_partitioning=True)
        except Exception:
            logger.warning("Direct partition scan failed — falling back to global LazyFrame.")
            return self.global_lf

    # ── Public API ───────────────────────────────────────────────────────

    def table_query(self, k: int = 1000, filters: list[dict] | None = None):
        """Scalar search with optional filters. Uses partition fast-path when possible."""
        lf = self._resolve_lazyframe(filters)

        if filters:
            # Only pass non-partition filters when we already resolved partitions via paths
            lf = apply_filters(lf, filters)

        return lf.head(k).select(["item_id", "offer_id"]).collect().to_dicts()

    def vector_search(self, query_vector, k: int = 5):
        """Performs a vector similarity search using LanceDB."""
        return self.vector_table.search(query_vector).limit(k).to_list()
