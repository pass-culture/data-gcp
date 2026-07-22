import duckdb
import pandas as pd
from loguru import logger

from src.constants import (
    H3_RESOLUTION,
    POI_SOURCE,
    POI_H3_INDEX_COL,
    POI_LATITUDE_COL,
    POI_LONGITUDE_COL,
)

# H3 ships as a DuckDB community extension, not a pip package.
H3_EXTENSION_INSTALL_SQL = "INSTALL h3 FROM community; LOAD h3;"


def get_connection(database_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with the H3 community extension loaded."""
    con = duckdb.connect(database_path)
    con.execute(H3_EXTENSION_INSTALL_SQL)
    return con


def load_dataframe(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> None:
    """Load a pandas DataFrame into a DuckDB table."""
    con.register("_load_dataframe_tmp", df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _load_dataframe_tmp")
    con.unregister("_load_dataframe_tmp")


def add_h3_index(
    con: duckdb.DuckDBPyConnection,
    table_name: str = POI_SOURCE,
    resolution: int = H3_RESOLUTION,
    latitude_col: str = POI_LATITUDE_COL,
    longitude_col: str = POI_LONGITUDE_COL,
    h3_index_col: str = POI_H3_INDEX_COL,
) -> None:
    """Add an H3 cell index column computed from a table's lat/lon columns."""
    logger.info(f"Adding H3 index column '{h3_index_col}' to table '{table_name}'...")
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {h3_index_col} UBIGINT")
    con.execute(
        f"""
        UPDATE {table_name}
        SET {h3_index_col} = h3_latlng_to_cell({latitude_col}, {longitude_col}, {resolution})
        WHERE {latitude_col} IS NOT NULL AND {longitude_col} IS NOT NULL
        """
    )
