"""Pure filter logic — no heavy dependencies (no GCS, LanceDB, HuggingFace)."""

import datetime

import polars as pl
from loguru import logger

DATE_COLS = ["offer_creation_date", "stock_beginning_date"]


def parse_date(val):
    if isinstance(val, datetime.date | datetime.datetime):
        return val
    try:
        return datetime.datetime.strptime(val, "%Y-%m-%d").date()
    except Exception as e:
        raise ValueError(
            f"Could not parse date value '{val}': {e}. " f"Expected format: YYYY-MM-DD."
        ) from e


def _cast_value(val, target_dtype, col):
    """Cast a filter value (or list of values) to match the Polars column dtype."""
    if col in DATE_COLS:
        if isinstance(val, list | tuple):
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
        if isinstance(val, list | tuple):
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
        if op == "between" and isinstance(val, list | tuple) and len(val) == 2:
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
