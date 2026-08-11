import logging
import time
from collections import defaultdict

import typer

from core.metabase_api import MetabaseAPI
from core.utils import (
    CLIENT_ID,
    METABASE_API_USERNAME,
    METABASE_HOST,
    PASSWORD,
    load_archiving_config,
    load_taxonomy_config,
)
from domain.archiving import (
    MoveToArchive,
    append_archiving_logs,
    archive_dead_dashboards,
    archive_empty_collections,
    compute_archive_stats,
    hard_archive_stale_cards,
    load_activity_dataframe,
    load_recently_failed_card_ids,
    log_archive_stats,
    select_soft_archive_candidates,
)
from domain.dependencies import run_dependencies
from domain.permissions import sync_permissions
from domain.taxonomy import run_taxonomy

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
def archive(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Log every action without mutating Metabase or writing to BigQuery.",
    ),
    dataset_name: str = typer.Option(
        None,
        "--dataset-name",
        help="Override the BigQuery dataset for the activity table "
        "(default: int_metabase_<env>). Useful for sandbox / pre-deploy testing.",
    ),
    table_name: str = typer.Option(
        "activity",
        "--table-name",
        help="Override the activity table name (default: 'activity').",
    ),
):
    """Archive cards based on archiving rules."""
    logger.info(
        "Starting archiving process (dry_run=%s, dataset=%s, table=%s)",
        dry_run,
        dataset_name,
        table_name,
    )
    metabase = _get_metabase_client()
    config = load_archiving_config()

    archive_collection_id = config["archive_collection_id"]
    max_cards = config.get("max_cards_to_archive", 50)
    soft_rules = config.get("soft_archive_rules", [])
    failure_cooldown_days = config.get("soft_archive_failure_cooldown_days", 30)

    activity_df = None
    excluded_ids = set()
    if soft_rules:
        activity_df = load_activity_dataframe(dataset=dataset_name, table=table_name)
        excluded_ids = load_recently_failed_card_ids(failure_cooldown_days)

    log_archive_stats(
        compute_archive_stats(activity_df, config, excluded_ids=excluded_ids)
    )

    if soft_rules:
        candidates = select_soft_archive_candidates(
            activity_df, soft_rules, excluded_ids=excluded_ids
        )

        log_entries = []
        moved = 0
        failed = []
        for card in candidates[:max_cards]:
            archiving = MoveToArchive(
                card=card,
                archive_collection_id=archive_collection_id,
                metabase=metabase,
                dry_run=dry_run,
            )
            log_entry = archiving.move_object()
            if log_entry is not None:
                log_entries.append(log_entry)
            # Only rename when the move actually succeeded — otherwise the card
            # stays in its original collection with a "[Archive] - ..." prefix.
            if dry_run or (log_entry and log_entry.get("status") == "success"):
                archiving.rename_archive_object()
                moved += 1
            else:
                failed.append((card["id"], card["name"]))
            if not dry_run:
                time.sleep(1)
        if not dry_run:
            append_archiving_logs(log_entries)

        logger.info("Soft-archive run: %d moved, %d failed", moved, len(failed))
        if failed:
            sample = failed[:5]
            logger.warning(
                "First %d failure(s) (id, name): %s%s",
                len(sample),
                sample,
                "" if len(failed) <= 5 else f" (and {len(failed) - 5} more)",
            )

    hard_archive_config = config.get("hard_archive_cards")
    if hard_archive_config:
        logger.info("Hard-archiving cards stale in archive folder...")
        hard_archive_stale_cards(
            metabase,
            name_pattern=hard_archive_config["name_pattern"],
            min_days_since_update=hard_archive_config["min_days_since_update"],
            max_cards=hard_archive_config["max_cards"],
            failure_cooldown_days=hard_archive_config.get("failure_cooldown_days", 30),
            dry_run=dry_run,
        )

    cleanup_config = config.get("empty_cleanup")
    root_ids = (cleanup_config or {}).get("scan_root_collection_ids", [])
    if cleanup_config and root_ids:
        min_age_days = cleanup_config["min_age_days"]
        min_days_since_update = cleanup_config.get("min_days_since_update")
        logger.info("Cleaning up dead dashboards...")
        archive_dead_dashboards(
            metabase,
            root_ids,
            min_age_days=min_age_days,
            min_days_since_update=min_days_since_update,
            dry_run=dry_run,
        )
        logger.info("Cleaning up empty collections...")
        archive_empty_collections(
            metabase,
            root_ids,
            min_age_days=min_age_days,
            min_days_since_update=min_days_since_update,
            dry_run=dry_run,
        )

    logger.info("Archiving complete")


@app.command()
def stats(
    dataset_name: str = typer.Option(
        None,
        "--dataset-name",
        help="Override BigQuery dataset for the activity table (default: int_metabase_<env>).",
    ),
    table_name: str = typer.Option(
        "activity",
        "--table-name",
        help="Override the activity table name (default: 'activity').",
    ),
):
    """Print archive pre-flight stats without mutating anything."""
    config = load_archiving_config()
    soft_rules = config.get("soft_archive_rules", [])
    cooldown_days = config.get("soft_archive_failure_cooldown_days", 30)
    activity_df = None
    excluded_ids = set()
    if soft_rules:
        activity_df = load_activity_dataframe(dataset=dataset_name, table=table_name)
        excluded_ids = load_recently_failed_card_ids(cooldown_days)
    log_archive_stats(
        compute_archive_stats(activity_df, config, excluded_ids=excluded_ids)
    )


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


@app.command()
def taxonomy(
    dataset_name: str = typer.Option(
        None,
        "--dataset-name",
        help="Override the source dataset holding the collection table "
        "(default: raw_<env>). Useful for sandbox / pre-deploy testing.",
    ),
    table_name: str = typer.Option(
        "metabase_collection",
        "--table-name",
        help="Override the source collection table name "
        "(default: 'metabase_collection').",
    ),
    destination_dataset: str = typer.Option(
        None,
        "--destination-dataset",
        help="Override the destination dataset for the taxonomy table "
        "(default: raw_<env>).",
    ),
    destination_table: str = typer.Option(
        "metabase_collection_taxonomy",
        "--destination-table",
        help="Override the destination taxonomy table name "
        "(default: 'metabase_collection_taxonomy').",
    ),
):
    """Resolve the collection taxonomy (squad/tier/certified) and write it to BigQuery."""
    logger.info(
        "Starting taxonomy resolution (source=%s.%s, destination=%s.%s)",
        dataset_name,
        table_name,
        destination_dataset,
        destination_table,
    )
    config = load_taxonomy_config()
    run_taxonomy(
        config,
        dataset=dataset_name,
        table=table_name,
        destination_dataset=destination_dataset,
        destination_table=destination_table,
    )
    logger.info("Taxonomy resolution complete")


@app.command()
def audit_duplicates():
    """Check which cards reference duplicate int_firebase_prod tables (Analytics vs Int Firebase Prod)."""
    logger.info("Starting duplicate table audit")
    metabase = _get_metabase_client()

    # 1. Get all tables and find duplicates
    all_tables = metabase.get_table()
    tables_by_key = defaultdict(list)
    for t in all_tables:
        key = (t["schema"], t["name"])
        tables_by_key[key].append({"table_id": t["id"], "db_id": t.get("db_id"), "active": t.get("active")})

    duplicates = {k: v for k, v in tables_by_key.items() if len(v) > 1}
    if not duplicates:
        logger.info("No duplicate tables found.")
        return

    logger.info("Found %d duplicate (schema, table) combos", len(duplicates))

    # Collect all duplicate table_ids
    dup_table_ids = {}
    for (schema, name), entries in duplicates.items():
        for entry in entries:
            dup_table_ids[entry["table_id"]] = {"schema": schema, "name": name, "db_id": entry["db_id"]}

    # 2. Get all cards and check which reference duplicate table_ids
    cards = metabase.get_cards()
    card_usage = defaultdict(list)  # table_id -> list of card info

    for card in cards:
        card_db_id = card.get("database_id")
        card_id = card["id"]
        card_name = card["name"]

        # Query builder cards: check source-table
        legacy_query = card.get("legacy_query")
        if legacy_query and isinstance(legacy_query, str):
            import json as _json
            try:
                legacy_query = _json.loads(legacy_query)
            except Exception:
                legacy_query = None

        if legacy_query and isinstance(legacy_query, dict):
            query_block = legacy_query.get("query", {})
            if isinstance(query_block, dict):
                source_table = query_block.get("source-table")
                if source_table and source_table in dup_table_ids:
                    card_usage[source_table].append({"card_id": card_id, "card_name": card_name, "card_db_id": card_db_id})
                # Check joins
                for join in query_block.get("joins", []):
                    jt = join.get("source-table")
                    if jt and jt in dup_table_ids:
                        card_usage[jt].append({"card_id": card_id, "card_name": card_name, "card_db_id": card_db_id, "via": "join"})

    # 3. Print results grouped by (schema, table) and db_id
    logger.info("=== AUDIT RESULTS ===")
    for (schema, name), entries in sorted(duplicates.items()):
        logger.info("--- %s.%s ---", schema, name)
        for entry in entries:
            tid = entry["table_id"]
            db_id = entry["db_id"]
            cards_for_tid = card_usage.get(tid, [])
            logger.info(
                "  db_id=%s  table_id=%s  -> %d card(s) using it",
                db_id, tid, len(cards_for_tid),
            )
            for c in cards_for_tid[:5]:
                logger.info(
                    "    card_id=%s  name=%r  db=%s%s",
                    c["card_id"], c["card_name"], c["card_db_id"],
                    f"  (via {c['via']})" if "via" in c else "",
                )
            if len(cards_for_tid) > 5:
                logger.info("    ... and %d more", len(cards_for_tid) - 5)

    logger.info("=== END AUDIT ===")


if __name__ == "__main__":
    app()
