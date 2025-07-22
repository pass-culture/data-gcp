import typer
import logging
from datetime import datetime, timedelta, timezone

from connectors.brevo import BrevoConnector
from http_custom.clients import SyncHttpClient
from jobs.brevo.utils import (
    SyncBrevoHeaderRateLimiter,
    get_api_configuration,
    etl_newsletter,
    etl_transactional
    
)
from jobs.brevo.config import UPDATE_WINDOW

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def run(
    target: str = typer.Option(
        ..., help="Task name (newsletter or transactional)"
    ),
    audience: str = typer.Option(..., help="Audience name (native or pro)"),
    start_date: str = typer.Option(None, help="Import start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="Import end date (YYYY-MM-DD)"),
):
    """Main entry point for Brevo ETL."""
    
    # Set up dates
    today = datetime.now(tz=timezone.utc)
    
    # Parse dates or use defaults
    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = today - timedelta(days=UPDATE_WINDOW)
    
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = today
    
    logger.info(
        f"Running Brevo ETL: target={target}, audience={audience}, "
        f"start={start_dt.strftime('%Y-%m-%d')}, end={end_dt.strftime('%Y-%m-%d')}"
    )
    
    # Get API configuration
    try:
        api_key, table_name = get_api_configuration(audience)
    except ValueError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)
    
    # Set up HTTP client and connector
    rate_limiter = SyncBrevoHeaderRateLimiter()
    client = SyncHttpClient(rate_limiter=rate_limiter)
    connector = BrevoConnector(api_key=api_key, client=client)
    
    # Process based on target
    if target == "newsletter":
        etl_newsletter(connector, audience, table_name, start_dt, end_dt)
    elif target == "transactional":
        # Note: Legacy code supports multiple API keys for parallel processing
        # This implementation uses a single API key. If you need the performance
        # benefits of parallel processing, you'll need to:
        # 1. Retrieve additional API keys
        # 2. Implement parallel template processing similar to legacy BrevoTransactional
        etl_transactional(connector, audience, start_dt, end_dt)
    else:
        typer.echo("Invalid target. Must be one of transactional/newsletter.")
        raise typer.Exit(code=1)
    
    logger.info("ETL process completed successfully.")


if __name__ == "__main__":
    app()