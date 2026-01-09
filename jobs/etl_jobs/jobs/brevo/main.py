import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import typer

# Decoupled internal imports
from factories.brevo import BrevoFactory
from jobs.brevo.config import UPDATE_WINDOW, get_api_configuration
from jobs.brevo.tasks import (
    run_async_transactional_etl,
    run_newsletter_etl,
    run_transactional_etl,
)

# Logging setup - silence noisy network libraries
for name in ("httpx", "httpcore", "urllib3", "google"):
    logging.getLogger(name).setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Brevo ETL Pipeline Orchestrator")


def parse_dates(
    start_date: Optional[str], end_date: Optional[str]
) -> Tuple[datetime, datetime]:
    """Helper to calculate date window or parse CLI input."""
    today = datetime.now(tz=timezone.utc)

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = today - timedelta(days=UPDATE_WINDOW)

    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = today

    if start_dt > end_dt:
        raise typer.BadParameter("Start date must be before end date.")

    return start_dt, end_dt


@app.command()
def run(
    target: str = typer.Option(..., help="Task name: 'newsletter' or 'transactional'"),
    audience: str = typer.Option(..., help="Audience target: 'native' or 'pro'"),
    start_date: str = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, help="End date (YYYY-MM-DD)"),
    async_mode: bool = typer.Option(
        False, "--async/--no-async", help="Use async processing (transactional only)"
    ),
    max_concurrent: int = typer.Option(
        5, help="Max concurrent requests for async processing"
    ),
):
    """Main Entry Point for Brevo ETL."""

    # 1. Resolve Execution Parameters
    start_dt, end_dt = parse_dates(start_date, end_date)

    # Use config helper to get table name (logic remains in config)
    _, table_name = get_api_configuration(audience)

    logger.info(
        f"🚀 Starting Brevo ETL | Target: {target} | Audience: {audience} | "
        f"Window: {start_dt.date()} to {end_dt.date()} | Async: {async_mode}"
    )

    # 2. Build Connector via Factory (Plumbing is hidden here)
    connector = BrevoFactory.create_connector(
        audience=audience, is_async=async_mode, max_concurrent=max_concurrent
    )

    # 3. Task Dispatcher
    try:
        if target == "newsletter":
            # Newsletter API doesn't benefit much from async due to small result set
            run_newsletter_etl(connector, audience, table_name, end_dt)

        elif target == "transactional":
            if async_mode:
                # Run the Async Task
                asyncio.run(
                    run_async_transactional_etl(connector, audience, start_dt, end_dt)
                )
            else:
                # Run the Sync Task
                run_transactional_etl(connector, audience, start_dt, end_dt)

        else:
            logger.error(f"Unknown target: {target}")
            raise typer.Exit(code=1)

        logger.info(f"✅ ETL {target} for {audience} completed successfully.")

    except Exception as e:
        logger.critical(f"❌ ETL Process Failed: {e}", exc_info=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
