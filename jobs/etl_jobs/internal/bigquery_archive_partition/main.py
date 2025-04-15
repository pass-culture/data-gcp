import typer
from typing import Any
import json
from archive import Archive
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def run(
    table: str = typer.Option(
        ...,
        help="Table name",
    ),
    config: str = typer.Option(
        ...,
        help="Config of the table",
    ),
    limit: int = typer.Option(
        1,
        help="Limit of partitions to export",
    ),
):
    try:
        config_dict: dict[str, Any] = json.loads(config)  # Use safe JSON parsing
    except json.JSONDecodeError as e:
        logger.error(f"Error: Invalid JSON config - {e}")
        raise typer.Exit(code=1)

    logger.info(
        f"Running archive for table {table} with config {config_dict}, limit {limit}"
    )
    archive = Archive(table, config_dict)
    archive.export_and_delete_partitions(limit)
    logger.info(f"Archive for table {table} completed successfully")


if __name__ == "__main__":
    typer.run(run)
