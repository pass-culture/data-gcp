import typer
from loguru import logger

from src.constants import H3_RESOLUTION, POI_SOURCE
from src.duckdb_load import add_h3_index, get_connection, load_dataframe
from src.utils.gcp import fetch_poi

app = typer.Typer()

@app.command()
def search_candidates(
    h3_resolution: int = typer.Option(H3_RESOLUTION),
) -> None:
    con = get_connection()

    logger.info("Loading POIs from BigQuery...")
    load_dataframe(con, fetch_poi(), POI_SOURCE)
    add_h3_index(con, resolution=h3_resolution)
    poi_count = con.execute(f"SELECT count(*) FROM {POI_SOURCE}").fetchone()[0]
    logger.info(f"Loaded and indexed {poi_count} POIs.")


if __name__ == "__main__":
    app()
