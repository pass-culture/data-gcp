import logging
import time

import typer

from core.metabase_api import MetabaseAPI
from core.utils import (
    CLIENT_ID,
    METABASE_API_USERNAME,
    METABASE_HOST,
    PASSWORD,
    load_archiving_config,
)
from domain.archiving import (
    ListArchive,
    MoveToArchive,
    archive_dead_dashboards,
    archive_empty_collections,
)
from domain.dependencies import run_dependencies
from domain.permissions import sync_permissions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = typer.Typer()


def _get_metabase_client():
    return MetabaseAPI(
        username=METABASE_API_USERNAME,
        password=PASSWORD,
        host=METABASE_HOST,
        client_id=CLIENT_ID,
    )


@app.command()
def archive():
    """Archive cards based on archiving rules."""
    logger.info("Starting archiving process")
    metabase = _get_metabase_client()
    config = load_archiving_config()

    rules_by_name = {rule["name"]: rule["sql"] for rule in config["rules"]}

    for folder in config["folders"]:
        rule_sql = rules_by_name.get(f"{folder}_archiving")
        if not rule_sql:
            logger.warning("No archiving rule found for folder '%s'", folder)
            continue

        list_archive = ListArchive(metabase_folder=folder, rule_sql=rule_sql)
        list_archive.get_data_archiving()
        cards = list_archive.preprocess_data_archiving()

        for card in cards[: config["max_cards_to_archive"]]:
            archiving = MoveToArchive(movement=card, metabase=metabase)
            log_entry = archiving.move_object()
            archiving.rename_archive_object()
            archiving.save_logs_bq(log_entry)
            time.sleep(1)

    cleanup_config = config.get("empty_collection_cleanup", {})
    root_ids = cleanup_config.get("root_collection_ids", [])
    if root_ids:
        logger.info("Cleaning up dead dashboards...")
        archive_dead_dashboards(metabase, root_ids)
        logger.info("Cleaning up empty collections...")
        archive_empty_collections(metabase, root_ids)

    logger.info("Archiving complete")


@app.command()
def permissions(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show changes without applying them"
    ),
):
    """Synchronize collection permissions from the environment config YAML."""
    logger.info("Starting permissions sync (dry_run=%s)", dry_run)
    metabase = _get_metabase_client()
    result = sync_permissions(metabase, dry_run=dry_run)
    logger.info(
        "Permissions sync done: %d change(s), applied=%s",
        len(result["changes"]),
        result["applied"],
    )


@app.command()
def dependencies():
    """Export card dependencies to BigQuery."""
    logger.info("Starting dependencies export")
    metabase = _get_metabase_client()
    run_dependencies(metabase)
    logger.info("Dependencies export complete")


if __name__ == "__main__":
    app()
