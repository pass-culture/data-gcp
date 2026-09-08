"""
Harvestr ETL Main Entry Point.

This module provides the main entry point for the Harvestr ETL job,
using the class-based architecture for better organization and maintainability.
"""

import typer
from harvestr.client import HarvestrClient
from harvestr.etl import HarvestrETL
from harvestr.utils import API_TOKEN
from loguru import logger


def main(
    start_date: str = typer.Option(
        ...,
        help="Start date for exporting datas (YYYY-MM-DD format).",
    ),
    end_date: str = typer.Option(
        ...,
        help="End date for exporting datas (YYYY-MM-DD format).",
    ),
) -> None:
    """
    Main entry point for Harvestr ETL job.
    """
    try:
        logger.info("Starting Harvestr ETL job...")
        logger.info(f"Date range: {start_date} to {end_date}")

        client = HarvestrClient(api_token=API_TOKEN)
        etl_processor = HarvestrETL(client)

        success = etl_processor.run_etl(start_date, end_date)

        if not success:
            logger.error("Harvestr ETL job did not complete successfully")
            raise typer.Exit(code=1)

        logger.info("Harvestr ETL job completed successfully")
    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Harvestr ETL job failed: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
