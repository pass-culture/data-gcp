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
from src.matching import (
    build_offerer_address_poi_link as compute_offerer_address_poi_link,
)
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

    logger.info(f"closest Candidates df summary:\n{candidates_df.loc[lambda df : df['distance_meters'] <= 50].groupby('offerer_address_id').min().describe(include='all')}")



    Path(candidates_output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(addresses_output_path).parent.mkdir(parents=True, exist_ok=True)
    candidates_df.to_parquet(candidates_output_path, index=False)
    addresses_df.to_parquet(addresses_output_path, index=False)
    logger.info(
        f"Wrote {len(candidates_df)} candidates to {candidates_output_path} "
        f"and {len(addresses_df)} offerer addresses to {addresses_output_path}"
    )


@app.command()
def build_offerer_address_poi_link(
    candidates_path: str = typer.Option("data/output/candidates.parquet"),
    addresses_path: str = typer.Option("data/output/addresses.parquet"),
    link_output_path: str = typer.Option("data/output/offerer_address_poi_link.parquet"),
) -> None:
    """Score geographic candidates for labeled offerer addresses and write the OffererAddressPOILink table.

    Reads the candidates/addresses parquet files produced by `search-candidates`,
    re-fetches POI data, and scores every remaining candidate pair on name similarity
    against both POI name columns -- no filtering beyond a non-empty offerer_address_label.
    """
    logger.info(f"Loading candidates from {candidates_path} and addresses from {addresses_path}...")
    candidates_df = pd.read_parquet(candidates_path)
    addresses_df = pd.read_parquet(addresses_path)

    logger.info("Fetching POIs from BigQuery...")
    poi_df = fetch_poi()

    link_df = compute_offerer_address_poi_link(candidates_df, addresses_df, poi_df)
    logger.info(f"Built OffererAddressPOILink table with {len(link_df)} rows.")

    Path(link_output_path).parent.mkdir(parents=True, exist_ok=True)
    link_df.to_parquet(link_output_path, index=False)
    logger.info(f"Wrote {len(link_df)} offerer_address/poi pairs to {link_output_path}")


if __name__ == "__main__":
    app()
