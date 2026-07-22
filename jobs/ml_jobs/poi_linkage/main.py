from pathlib import Path

import pandas as pd
import typer
from loguru import logger

from src.candidates_retrieval import find_candidates
from src.constants import (
    ADDRESS_LATITUDE_COL,
    ADDRESS_LONGITUDE_COL,
    APPLICATIVE_SOURCE_TABLE,
    BATCH_SIZE,
    H3_RESOLUTION,
    POI_SOURCE_TABLE,
    SEARCH_RADIUS_METERS,
)
from src.duckdb_load import add_h3_index, get_connection, load_dataframe
from src.utils.gcp import fetch_poi, iter_offerer_address_batches

app = typer.Typer()

@app.command()
def search_candidates(
    h3_resolution: int = typer.Option(H3_RESOLUTION),
    radius_m: float = typer.Option(SEARCH_RADIUS_METERS),
    address_batch_size: int = typer.Option(BATCH_SIZE),
    candidates_output_path: str = typer.Option("data/output/candidates.parquet"),
    addresses_output_path: str = typer.Option("data/output/addresses.parquet"),
) -> None:
    con = get_connection()

    logger.info("Loading POIs from BigQuery...")
    load_dataframe(con, fetch_poi(), POI_SOURCE_TABLE)
    add_h3_index(con, resolution=h3_resolution)
    poi_count = con.execute(f"SELECT count(*) FROM {POI_SOURCE_TABLE}").fetchone()[0]
    logger.info(f"Loaded and indexed {poi_count} POIs.")

    address_batches = iter_offerer_address_batches(batch_size=address_batch_size)

    candidates_dfs = []
    addresses_dfs = []
    for batch_index, addresses_df in enumerate(address_batches):
        logger.info(
            f"Batch {batch_index}: searching candidates for {len(addresses_df)} offerer addresses..."
        )
        addresses_dfs.append(addresses_df)
        load_dataframe(con, addresses_df, APPLICATIVE_SOURCE_TABLE)
        add_h3_index(
            con,
            table_name=APPLICATIVE_SOURCE_TABLE,
            resolution=h3_resolution,
            latitude_col=ADDRESS_LATITUDE_COL,
            longitude_col=ADDRESS_LONGITUDE_COL,
        )
        candidates_df = find_candidates(con, radius_m=radius_m, resolution=h3_resolution)
        if not candidates_df.empty:
            candidates_dfs.append(candidates_df)

    if not candidates_dfs:
        logger.warning("No geographic candidates found.")
        return

    candidates_df = pd.concat(candidates_dfs, ignore_index=True)
    addresses_df = pd.concat(addresses_dfs, ignore_index=True)

    logger.info(f"Candidates df summary:\n{candidates_df.describe(include='all')}")
    Path(candidates_output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(addresses_output_path).parent.mkdir(parents=True, exist_ok=True)
    candidates_df.to_parquet(candidates_output_path, index=False)
    addresses_df.to_parquet(addresses_output_path, index=False)
    logger.info(
        f"Wrote {len(candidates_df)} candidates to {candidates_output_path} "
        f"and {len(addresses_df)} offerer addresses to {addresses_output_path}"
    )



if __name__ == "__main__":
    app()
