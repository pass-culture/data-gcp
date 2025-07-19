import asyncio
import logging
from datetime import datetime, timedelta
import sys

import typer
from google.cloud import bigquery

from config import Config, SCHEMAS
from etl import TransactionalExtractor, NewsletterExtractor

# Set up more detailed logging
def setup_logging(verbose: bool = False):
    """Configure logging with optional verbose mode"""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            # Optionally add file handler
            # logging.FileHandler(f'brevo_etl_{datetime.now():%Y%m%d_%H%M%S}.log')
        ]
    )
    
    # Adjust log levels for noisy libraries
    if not verbose:
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("google").setLevel(logging.WARNING)

app = typer.Typer()

async def run_transactional_etl(config: Config, start_date: str, end_date: str):
    """Run transactional email ETL"""
    start_time = datetime.now()
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting transactional ETL for {start_date} to {end_date}")
    
    extractor = TransactionalExtractor(config, start_date, end_date)
    df = await extractor.extract()
    
    extraction_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Extraction completed in {extraction_time:.2f} seconds")
    
    if df is None or df.empty:
        logger.warning("No data extracted")
        return "No data to load"
    
    # Save to BigQuery
    logger.info(f"Loading {len(df)} rows to BigQuery...")
    client = bigquery.Client()
    table_id = f"{config.gcp_project}.{config.tmp_dataset}.transactional_{datetime.now():%Y%m%d}"
    
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    
    total_time = (datetime.now() - start_time).total_seconds()
    
    return f"Loaded {len(df)} rows to {table_id} in {total_time:.2f} seconds (extraction: {extraction_time:.2f}s)"

@app.command()
def main(
    target: str = typer.Option(..., help="Target: newsletter or transactional"),
    audience: str = typer.Option(..., help="Audience: native or pro"),
    start_date: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(..., help="End date (YYYY-MM-DD)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging")
):
    """Main ETL command"""
    setup_logging(verbose)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting ETL: target={target}, audience={audience}, dates={start_date} to {end_date}")
    
    config = Config.from_environment(audience)
    
    if target == "transactional":
        result = asyncio.run(run_transactional_etl(config, start_date, end_date))
    elif target == "newsletter":
        # Similar for newsletter
        result = "Newsletter ETL not implemented yet"
    else:
        raise ValueError(f"Unknown target: {target}")
    
    logger.info(f"ETL completed: {result}")
    print(result)

if __name__ == "__main__":
    app()