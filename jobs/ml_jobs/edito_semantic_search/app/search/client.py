import datetime

import lancedb
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger
from pyarrow.fs import GcsFileSystem

from app.constants import PARQUET_FILE, embedding_model

DATE_COLS = ["offer_creation_date", "stock_beginning_date"]
PARTITION_COLS = ["offer_subcategory_id"]


def parse_date(val):
    if isinstance(val, (datetime.date, datetime.datetime)):
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
        **dict.fromkeys(
            [
                pl.Int8,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.UInt8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
            ],
            int,
        ),
        **dict.fromkeys([pl.Float32, pl.Float64], float),
        **dict.fromkeys([pl.String, pl.Utf8], str),
        **dict.fromkeys([pl.Boolean], bool),
    }

    cast_fn = type_map.get(target_dtype)
    if cast_fn is None:
        return val

    try:
        if isinstance(val, (list, tuple)):
            return [cast_fn(v) for v in val]
        return cast_fn(val)
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Cannot cast '{val}' to {cast_fn.__name__} for column '{col}'"
        ) from e


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
    expressions = []

    for f in filters:
        col, op, val = f["column"], f["operator"].lower(), f["value"]

        if col not in schema:
            logger.warning(f"Column '{col}' not found in dataset — skipping.")
            continue

        val = _cast_value(val, schema[col], col)

        # Handle 'between' operator efficiently
        if op == "between" and isinstance(val, (list, tuple)) and len(val) == 2:
            expressions.append(pl.col(col).is_between(val[0], val[1]))
            continue

        filter_fn = OPERATOR_MAP.get(op)
        if filter_fn is None:
            raise ValueError(f"Unsupported operator: '{op}'")

        expressions.append(filter_fn(pl.col(col), val))

    if expressions:
        # Apply all filter expressions at once for an optimized query plan
        lf = lf.filter(*expressions)

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
        logger.info("Loading global dataset for fast partition pruning...")
        logger.info(f"Using parquet file: {PARQUET_FILE}")
        
        gcs = GcsFileSystem()
        gcs_path = PARQUET_FILE.replace("gs://", "")
        
        # NOTE: Ensure these types match your actual data! 
        # I assumed offer_subcategory_id is an Int64. If it's a string, change it to pa.string().
        hive_partitioning = ds.partitioning(
            flavor="hive",
            schema=pa.schema([
                ("offer_subcategory_id", pa.string())
            ])
        )

        self.dataset = ds.dataset(
            gcs_path,
            filesystem=gcs,
            format="parquet",
            partitioning=hive_partitioning,
        )
        self.global_lf = pl.scan_pyarrow_dataset(self.dataset)

    # ── Public API ───────────────────────────────────────────────────────

    def table_query(self, k: int = 1000, filters: list[dict] | None = None):
        """Scalar search with optional filters. Relies on native Polars partition pruning."""
        lf = self.global_lf

        if filters:
            lf = apply_filters(lf, filters)

        return lf.head(k).select(["item_id", "offer_id"]).collect().to_dicts()

    def vector_search(self, query_vector, k: int = 5):
        """Performs a vector similarity search using LanceDB."""
        return (
            self.vector_table.search(query_vector)
            .nprobes(10)
            .select(
                [
                    "id",
                    "offer_name",
                    "offer_description",
                    "offer_subcategory_id",
                ]
            )
            .limit(k)
            .to_list()
        )