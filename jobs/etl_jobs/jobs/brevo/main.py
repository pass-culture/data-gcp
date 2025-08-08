import asyncio
import logging
from datetime import datetime, timedelta, timezone

import typer
from connectors.brevo import (
    create_async_brevo_connector,
    create_brevo_connector,
)
from jobs.brevo.config import UPDATE_WINDOW
from jobs.brevo.utils import (
    async_etl_transactional,
    etl_newsletter,
    etl_transactional,
    get_api_configuration,
)

# Logging setup
for name in ("httpx", "httpcore", "urllib3"):
    logging.getLogger(name).setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def run(
    target: str = typer.Option(..., help="Task name (newsletter or transactional)"),
    audience: str = typer.Option(..., help="Audience name (native or pro)"),
    start_date: str = typer.Option(None, help="Import start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="Import end date (YYYY-MM-DD)"),
    async_mode: bool = typer.Option(
        False,
        "--async/--no-async",
        help="Use async processing (transactional only)",
        show_default=True,
    ),
    max_concurrent: int = typer.Option(
        5, "--max-concurrent", help="Max concurrent requests for async processing"
    ),
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

    assert start_dt <= end_dt, "Start date must be before end date."

    fallback_flag = async_mode and target == "newsletter"
    async_fallback = async_mode and target == "transactional"
    logger.info(
        f"Running Brevo ETL: target={target}, audience={audience}, "
        f"start={start_dt.strftime('%Y-%m-%d')}, end={end_dt.strftime('%Y-%m-%d')}, "
        f"{'WARNING: Async mode disabled: only supported for transactional target! ' if fallback_flag else ''}"
        f"async={async_fallback}, max_concurrent={max_concurrent if async_fallback else 'N/A'} "
    )

    # Get API configuration
    try:
        api_key, table_name = get_api_configuration(audience)
    except ValueError as e:
        typer.echo(str(e))
        raise typer.Exit(code=1)

    # Process based on target
    if target == "newsletter":
        connector = create_brevo_connector(api_key)
        etl_newsletter(connector, audience, table_name, start_dt, end_dt)

    elif target == "transactional":
        if async_mode:
            asyncio.run(
                _run_async_transactional(
                    api_key, audience, start_dt, end_dt, max_concurrent=max_concurrent
                )
            )
        else:
            connector = create_brevo_connector(api_key)
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

    async with create_async_brevo_connector(
        api_key, max_concurrent=max_concurrent
    ) as connector:
        await async_etl_transactional(connector, audience, start_dt, end_dt)


if __name__ == "__main__":
    app()
