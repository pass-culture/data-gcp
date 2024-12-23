import polars as pl
import typer
from google.cloud import bigquery
from loguru import logger

from utils import ENV_SHORT_NAME, export_polars_to_bq, get_offline_recos


def offline_recommendation(
    input_table: str = typer.Option(
        ..., help="Table name with data for offline recommendations"
    ),
    output_table: str = typer.Option(
        ..., help="Output table for offline recommendations"
    ),
):
    """
    Fetches data from BigQuery, gets offline recommendations, and stores the results back to BigQuery.

    Args:
        input_table (str): The name of the input table containing data for offline recommendations.
        output_table (str): The name of the output table for storing offline recommendations.
    """
    with bigquery.Client() as client:
        try:
            logger.info("Offline recommendation: fetch data...")
            data = pl.from_arrow(
                client.query(f"SELECT * FROM `tmp_{ENV_SHORT_NAME}.{input_table}` ")
                .result()
                .to_arrow()
            )

            logger.info(
                f"Offline recommendation: Get recommendations from API... for {len(data)} users"
            )
            offline_recommendations = get_offline_recos(data)

            logger.info("Offline recommendation: Store recos to BQ...")
            export_polars_to_bq(
                client=client,
                data=offline_recommendations,
                dataset=f"tmp_{ENV_SHORT_NAME}",
                output_table=output_table,
            )
            logger.info(f"Offline recommendation: Exported to {output_table}")
        except Exception as e:
            logger.error(f"Offline recommendation failed: {e}")
    return


if __name__ == "__main__":
    typer.run(offline_recommendation)
