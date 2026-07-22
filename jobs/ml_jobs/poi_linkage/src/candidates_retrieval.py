import math

import duckdb
import pandas as pd
from loguru import logger

from src.constants import (
    ADDRESS_H3_INDEX_COL,
    ADDRESS_LATITUDE_COL,
    ADDRESS_LONGITUDE_COL,
    APPLICATIVE_SOURCE_TABLE,
    DISTANCE_METERS_COL,
    H3_RESOLUTION,
    OFFERER_ADDRESS_ID_COL,
    POI_H3_INDEX_COL,
    POI_ID_COL,
    POI_LATITUDE_COL,
    POI_LONGITUDE_COL,
    POI_SOURCE_TABLE,
    SEARCH_RADIUS_METERS,
)


def _grid_disk_k(con: duckdb.DuckDBPyConnection, resolution: int, radius_m: float) -> int:
    """Smallest grid-disk ring size guaranteed to cover radius_m at the given H3 resolution.

    h3_grid_disk selects whole hexagons, which only approximates a circle, so the
    candidate set it returns must still be filtered by exact distance afterwards.
    """
    logger.info(f"Computing grid-disk ring size k for radius {radius_m} m at resolution {resolution}...")
    edge_length_m = con.execute(
        f"SELECT h3_get_hexagon_edge_length_avg({resolution}, 'm')"
    ).fetchone()[0]
    logger.info(f"Average edge length at resolution {resolution} is {edge_length_m:.2f} m.")
    return math.ceil(radius_m / edge_length_m) + 1


def find_candidates(
    con: duckdb.DuckDBPyConnection,
    addresses_table: str = APPLICATIVE_SOURCE_TABLE,
    poi_table: str = POI_SOURCE_TABLE,
    radius_m: float = SEARCH_RADIUS_METERS,
    resolution: int = H3_RESOLUTION,
) -> pd.DataFrame:
    """Find POI candidates within radius_m of each offerer_address using H3 grid-disk lookup.

    Both addresses_table and poi_table must already have an h3_index column
    (see src.duckdb_store.add_h3_index) computed at the given resolution.

    Returns one row per (offerer_address_id, poi_id, distance_meters) pair within the radius.
    """
    k = _grid_disk_k(con, resolution=resolution, radius_m=radius_m)

    logger.info(f"Finding POI candidates within {radius_m} m of each offerer_address")
    return con.execute(
        f"""
        SELECT * FROM (
            SELECT
                a.{OFFERER_ADDRESS_ID_COL},
                p.{POI_ID_COL},
                h3_great_circle_distance(
                    a.{ADDRESS_LATITUDE_COL}, a.{ADDRESS_LONGITUDE_COL},
                    p.{POI_LATITUDE_COL}, p.{POI_LONGITUDE_COL},
                    'm'
                ) AS {DISTANCE_METERS_COL}
            FROM {addresses_table} a, UNNEST(h3_grid_disk(a.{ADDRESS_H3_INDEX_COL}, {k})) AS candidate_cells(cell)
            JOIN {poi_table} p ON p.{POI_H3_INDEX_COL} = candidate_cells.cell
        )
        WHERE {DISTANCE_METERS_COL} <= {radius_m}
        """
    ).fetchdf()
