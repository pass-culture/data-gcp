import typer
import polars as pl
from loguru import logger
from google.cloud import bigquery
from utils import ENV_SHORT_NAME, get_offline_recos, export_polars_to_bq


def offline_recommendation(
    input_table: str = typer.Option(
        ..., help="Table name with data for offline recommendations"
    ),
    output_table: str = typer.Option(
        ..., help="Output table for offline recommendations"
    ),
):
    client = bigquery.Client()
    logger.info("Offline recommendation: fetch data...")
    data = pl.from_arrow(
        client.query(f"SELECT * FROM `tmp_{ENV_SHORT_NAME}.{input_table}` ")
        .result()
        .to_arrow()
    )

    logger.info("Offline recommendation: Get recommendations from API...")
    offline_recommendations = get_offline_recos(data)

    logger.info("Offline recommendation: Store recos to BQ...")
    export_polars_to_bq(
        client=client,
        data=offline_recommendations,
        dataset=f"tmp_{ENV_SHORT_NAME}",
        output_table=output_table,
    )
    logger.info(f"Offline recommendation: Exported to {output_table}")
    return


if __name__ == "__main__":
    typer.run(offline_recommendation)
