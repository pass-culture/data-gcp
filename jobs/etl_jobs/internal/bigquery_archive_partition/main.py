import typer
from typing import Any, Optional
import json
from archive import Archive, JobConfig
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def run(
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
        config: JobConfig = JobConfig(**json.loads(config))
    except json.JSONDecodeError as e:
        logger.error(f"Error: Invalid JSON config - {e}")
        raise typer.Exit(code=1)

    logger.info(
        f"Running archive for table {config.table_id} with config {config}, limit {limit}"
    )
    archive = Archive(config)
    archive.archive_partitions(limit)
    logger.info(f"Archive for table {config.table_id} completed successfully")


if __name__ == "__main__":
    typer.run(run)
