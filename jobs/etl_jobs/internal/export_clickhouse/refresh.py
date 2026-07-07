import json
import logging
from typing import Optional

import typer

from core.fs import load_sql
from core.utils import ENV_SHORT_NAME, get_clickhouse_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def run(
    table_name: str = typer.Option(
        ...,
        help="table_name",
    ),
    folder: str = typer.Option(
        "analytics",
        help="folder_name",
    ),
    ch_session_settings: Optional[str] = typer.Option(
        None, help="JSON string of ClickHouse session settings"
    ),
):
    sql_query = load_sql(
        table_name=table_name,
        folder=folder,
        extra_data={"env_short_name": ENV_SHORT_NAME},
    )
    parsed_settings = json.loads(ch_session_settings) if ch_session_settings else None

    logger.info("Will Execute:")
    logger.info(sql_query)
    logger.info(f"Refresh {table_name}...")

    if parsed_settings:
        logger.info(f"With query settings: {parsed_settings}")

    try:
        client = get_clickhouse_client(settings=parsed_settings)
    except Exception as e:
        logger.error(f"Failed to create ClickHouse client: {e}")
        raise RuntimeError("Failed to create ClickHouse client") from e

    try:
        client.command(sql_query)
    except Exception as e:
        logger.error(f"Failed to refresh table {table_name!r}: {e}")
        raise RuntimeError(f"Failed to refresh table {table_name!r}") from e


if __name__ == "__main__":
    app()
