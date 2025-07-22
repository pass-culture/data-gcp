import asyncio
import logging
from datetime import datetime, timedelta, timezone

import typer
from connectors.brevo import AsyncBrevoConnector, BrevoConnector
from http_custom.clients import AsyncHttpClient, SyncHttpClient
from jobs.brevo.config import UPDATE_WINDOW
from jobs.brevo.utils import (
    AsyncBrevoHeaderRateLimiter,
    SyncBrevoHeaderRateLimiter,
    async_etl_transactional,
    etl_newsletter,
    etl_transactional,
    get_api_configuration,
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def run(
    target: str = typer.Option(..., help="Task name (newsletter or transactional)"),
    audience: str = typer.Option(..., help="Audience name (native or pro)"),
    start_date: str = typer.Option(None, help="Import start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="Import end date (YYYY-MM-DD)"),
    use_async: bool = typer.Option(
        True, "--async", help="Use async processing (transactional only)"
    ),
    max_concurrent: int = typer.Option(
        5, "--max-concurrent", help="Max concurrent requests for async processing"
    ),
):
    """Main entry point for Brevo ETL."""
    async_mode = False
    if use_async:
        async_mode = True
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

    warn_flag = async_mode and target == "newsletter"
    legit_async = async_mode and target == "transactional"
    logger.info(
        f"Running Brevo ETL: target={target}, audience={audience}, "
        f"start={start_dt.strftime('%Y-%m-%d')}, end={end_dt.strftime('%Y-%m-%d')}, "
        f"{'WARNING: Async mode disabled: only supported for transactional target! ' if warn_flag else ''}"
        f"async={legit_async}, max_concurrent={max_concurrent if legit_async else 'N/A'} "
    )

    # Get API configuration
    try:
        api_key, table_name = get_api_configuration(audience)
    except ValueError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)

    # Process based on target
    if target == "newsletter":
        # Set up sync HTTP client and connector
        rate_limiter = SyncBrevoHeaderRateLimiter()
        client = SyncHttpClient(rate_limiter=rate_limiter)
        connector = BrevoConnector(api_key=api_key, client=client)

        etl_newsletter(connector, audience, table_name, start_dt, end_dt)

    elif target == "transactional":
        if async_mode:
            # Run async version
            asyncio.run(
                _run_async_transactional(
                    api_key, audience, start_dt, end_dt, max_concurrent=max_concurrent
                )
            )
        else:
            # Set up sync HTTP client and connector
            rate_limiter = SyncBrevoHeaderRateLimiter()
            client = SyncHttpClient(rate_limiter=rate_limiter)
            connector = BrevoConnector(api_key=api_key, client=client)

            etl_transactional(connector, audience, start_dt, end_dt)
    else:
        typer.echo("Invalid target. Must be one of transactional/newsletter.")
        raise typer.Exit(code=1)

    logger.info(f"ETL {target} process for audience {audience} completed successfully.")


async def _run_async_transactional(
    api_key: str,
    audience: str,
    start_dt: datetime,
    end_dt: datetime,
    max_concurrent: int = 5,
):
    """Helper function to run async transactional ETL."""
    # Set up async HTTP client and connector with concurrency control
    rate_limiter = AsyncBrevoHeaderRateLimiter(max_concurrent=max_concurrent)

    async with AsyncHttpClient(rate_limiter=rate_limiter) as client:
        connector = AsyncBrevoConnector(api_key=api_key, client=client)
        await async_etl_transactional(connector, audience, start_dt, end_dt)


if __name__ == "__main__":
    app()
