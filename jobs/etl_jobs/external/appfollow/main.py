"""
AppFollow ETL Main Entry Point.

This module provides the main entry point for the AppFollow ETL job,
using the class-based architecture for better organization and maintainability.
"""

import typer
from loguru import logger

from client import AppFollowClient
from etl import AppFollowETL
from utils import API_TOKEN


def main(
    start_date: str = typer.Option(
        ...,
        help="Start date for exporting reviews (YYYY-MM-DD format).",
    ),
    end_date: str = typer.Option(
        ...,
        help="End date for exporting reviews (YYYY-MM-DD format).",
    ),
    ext_id: str = typer.Option(
        ...,
        help="App external ID (package name for Android, numeric ID for IOS",
    ),
) -> None:
    """
    Main entry point for AppFollow ETL job.
    """
    try:
        logger.info("Starting AppFollow ETL job...")
        logger.info(f"Target App ID: {ext_id}")
        logger.info(f"Date range: {start_date} to {end_date}")

        client = AppFollowClient(api_token=API_TOKEN)
        etl_processor = AppFollowETL(client)
        success = etl_processor.run_etl(
            ext_id=ext_id, from_date=start_date, to_date=end_date
        )

        if success:
            logger.success("AppFollow ETL job completed successfully!")
        else:
            logger.error("AppFollow ETL job failed during execution.")
            raise typer.Exit(code=1)
    except typer.Exit:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in AppFollow ETL job: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
