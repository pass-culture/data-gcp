"""Typer CLI entry point for the Metabase migration tool.

Provides the `migrate` command with `--dry-run` support.
Orchestrates the full migration flow:
1. Discover impacted cards (via Metabase API)
2. Build field/table mappings (via Metabase API)
3. Migrate each card (native SQL + query builder)
4. Log results to BigQuery
"""

from __future__ import annotations

import datetime
import json
import logging
from pathlib import Path
from typing import Any

import typer
from google.cloud import bigquery
from pydantic import ValidationError

from api.client import DATABASE_TABLES_CACHE_PATH, MetabaseClient
from api.models import Card, TablesToMigrate
from config import (
    INT_METABASE_DATASET,
    METABASE_API_USERNAME,
    PROJECT_NAME,
    get_iap_bearer_token,
    get_metabase_host,
    get_metabase_password,
)
from discovery.mapping import build_field_mapping, build_table_mapping
from discovery.metabase import (
    _sql_references_table,
    build_card_dependency_cache,
    build_dashboard_dependency_cache,
    get_impacted_cards_from_cache,
)
from migration.card import _get_stage_type, migrate_card, migrate_dashboard

app = typer.Typer(name="metabase-migration", help="Migrate Metabase cards after BigQuery table/column renames.")


@app.callback()
def _callback() -> None:
    """Metabase migration tool."""


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

TABLES_TO_MIGRATE_PATH = Path("data/tables-to-migrate.json")


def _card_references_legacy_table(
    card: Any, legacy_table_name: str, legacy_schema_name: str, legacy_table_id: int | None
) -> bool:
    """Check whether a card references the legacy table.

    For native SQL cards, checks if the legacy table name appears in the SQL text (schema-aware).
    For query builder cards, checks if the source-table ID matches the legacy table ID.

    Args:
        card: The Card object.
        legacy_table_name: The legacy table name to search for.
        legacy_schema_name: The legacy schema name to search for.
        legacy_table_id: The legacy table's Metabase ID (may be None if not found).

    Returns:
        True if the card references the legacy table, False otherwise.
    """
    dq = card.dataset_query
    if dq is None or not dq.stages:
        return False

    stage = dq.stages[0]
    stage_type = stage.get("lib/type")

    if stage_type == "mbql.stage/native":
        sql = stage.get("native", "")
        return _sql_references_table(sql, legacy_table_name, legacy_schema_name)

    if stage_type == "mbql.stage/mbql":
        if legacy_table_id is None:
            return False
        source_table = stage.get("source-table")
        return bool(source_table == legacy_table_id)

    return False


@app.command()
def migrate(
    dry_run: bool = typer.Option(False, "--dry-run", help="Show diffs without writing changes to Metabase"),
    tables_file: str = typer.Option(
        str(TABLES_TO_MIGRATE_PATH), "--tables-file", help="Path to tables-to-migrate JSON file"
    ),
    card_ids_str: str = typer.Option(
        "", "--card-ids", help="Comma-separated card IDs to migrate (skips API discovery)"
    ),
    database_name: str = typer.Option(..., help="Name of the Metabase database to search tables in"),
) -> None:
    """Migrate Metabase cards from legacy tables to new tables.

    Discovers impacted cards via the Metabase API, builds field mappings
    by matching column names, and updates each card.

    Supports both native SQL cards and query builder cards in a
    single pass. Use --dry-run to preview changes without writing.
    """
    # Determine if we're using local card IDs (no API discovery)
    use_api_discovery = not card_ids_str

    # --- Load and validate tables-to-migrate JSON ---
    tables_path = Path(tables_file)
    if not tables_path.exists():
        logger.error("Tables-to-migrate file not found at %s", tables_path)
        raise typer.Exit(code=1)

    with open(tables_path) as f:
        raw = json.load(f)

    try:
        tables_to_migrate = TablesToMigrate.model_validate(raw)
    except ValidationError as e:
        logger.error("Invalid tables-to-migrate file: %s", e)
        raise typer.Exit(code=1)

    logger.info("Loaded %d table migration entries", len(tables_to_migrate.root))

    # --- Connect to services ---
    logger.info("Connecting to Metabase...")
    metabase_host = get_metabase_host()
    metabase_password = get_metabase_password()
    bearer_token = get_iap_bearer_token()
    client = MetabaseClient.from_credentials(
        host=metabase_host, username=METABASE_API_USERNAME, password=metabase_password, bearer_token=bearer_token
    )

    # --- Build table catalog (includes inactive/deleted tables) ---
    if DATABASE_TABLES_CACHE_PATH.exists():
        DATABASE_TABLES_CACHE_PATH.unlink()

    database_id = client.find_database_id(database_name)
    if database_id is None:
        logger.error("Database '%s' not found in Metabase", database_name)
        raise typer.Exit(code=1)

    table_catalog = client.build_table_catalog(database_id)

    global_impacted_card_ids = set()

    # --- Pre-build card dependency cache (once for all tables) ---
    if not card_ids_str:
        logger.info("Building card dependency cache for all tables...")
        _card_dep_cache = build_card_dependency_cache(client, table_catalog)
        for legacy_key in tables_to_migrate.root.keys():
            legacy_table = legacy_key.split(".")[1]
            for impacted_card_id in get_impacted_cards_from_cache(legacy_table, _card_dep_cache):
                global_impacted_card_ids.add(str(impacted_card_id))
    else:
        _card_dep_cache = None
        for impacted_card_id in card_ids_str.split(","):
            global_impacted_card_ids.add(impacted_card_id.strip())

    logger.info("Building dashboard dependency cache for %d targeted cards...", len(global_impacted_card_ids))
    _dash_dep_cache = build_dashboard_dependency_cache(client, global_impacted_card_ids)

    # --- Per-table migration loop ---
    any_table_failed = False
    global_success = 0
    global_failure = 0

    for legacy_key, entry in tables_to_migrate.root.items():
        # Parse legacy schema and table from key
        legacy_schema, legacy_table = legacy_key.split(".")

        # Determine new schema and table
        if entry.target_table is not None:
            new_schema, new_table = entry.target_table.split(".")
        else:
            # columns-only migration — same table
            new_schema, new_table = legacy_schema, legacy_table

        column_mapping = entry.columns_to_migrate or {}

        # Log migration type
        if entry.target_table is None:
            logger.info("Table '%s': columns-only migration (%d columns)", legacy_key, len(column_mapping))
        elif entry.columns_to_migrate is None:
            logger.info("Table '%s': table rename only → %s", legacy_key, entry.target_table)
        else:
            logger.info("Table '%s' → '%s' (%d columns)", legacy_key, entry.target_table, len(column_mapping))

        # --- Discover impacted cards ---
        if card_ids_str:
            requested_ids = [int(cid.strip()) for cid in card_ids_str.split(",") if cid.strip()]
            logger.info("Using provided card IDs (API discovery skipped)")

            # Fetch each card to verify it exists
            found_cards: list[Card] = []
            not_found_ids: list[int] = []
            for cid in requested_ids:
                try:
                    found_cards.append(client.get_card(cid))
                except Exception:
                    not_found_ids.append(cid)

            if not_found_ids:
                logger.warning(
                    "Found %d cards out of %d provided (card IDs not found: %s)",
                    len(found_cards),
                    len(requested_ids),
                    not_found_ids,
                )
            else:
                logger.info("All %d cards found", len(found_cards))

            # Resolve legacy table ID early so we can check which cards are impacted
            legacy_table_id_for_check = client.find_table_id(legacy_table, legacy_schema)

            # Filter to only cards that actually reference the legacy table
            impacted_cards = [
                card
                for card in found_cards
                if _card_references_legacy_table(card, legacy_table, legacy_schema, legacy_table_id_for_check)
            ]
            card_ids = [card.id for card in impacted_cards if card.id is not None]
            logger.info(
                "%d cards impacted out of %d found for table '%s'",
                len(card_ids),
                len(found_cards),
                legacy_key,
            )
        else:
            logger.info("Looking up impacted cards for table '%s' from cache...", legacy_key)
            card_ids = get_impacted_cards_from_cache(legacy_table, _card_dep_cache)
            logger.info("Found %d impacted cards for table '%s'", len(card_ids), legacy_key)
            if len(card_ids) > 0:
                logger.info(f"IDs : {list(card_ids.keys()) if hasattr(card_ids, 'keys') else list(card_ids)}")
                logger.info("==============================================================")

        if not card_ids:
            logger.info("No cards to migrate for table '%s'. Skipping.", legacy_key)
            continue

        # --- Build mappings ---
        logger.info("Building field mappings for table '%s'...", legacy_key)
        legacy_table_id = client.find_table_id(legacy_table, legacy_schema)
        new_table_id = client.find_table_id(new_table, new_schema)

        if legacy_table_id is None:
            logger.error("Legacy table '%s.%s' not found in Metabase", legacy_schema, legacy_table)
            any_table_failed = True
            continue

        if new_table_id is None:
            logger.error("New table '%s.%s' not found in Metabase", new_schema, new_table)
            any_table_failed = True
            continue

        field_mapping = build_field_mapping(
            metabase_client=client,
            legacy_table_id=legacy_table_id,
            new_table_id=new_table_id,
            column_mapping=column_mapping,
        )
        is_table_rename = entry.target_table is not None and entry.target_table != legacy_key
        table_mapping = build_table_mapping(legacy_table_id, new_table_id) if is_table_rename else {}

        logger.info("Mappings for '%s': %d fields, %d tables", legacy_key, len(field_mapping), len(table_mapping))

        # --- Migrate cards ---
        table_logs: list[dict[str, Any]] = []
        table_success = 0
        table_failure = 0

        for card_id in card_ids:
            log_entry: dict[str, Any] = {
                "card_id": card_id,
                "legacy_table_name": legacy_table,
                "new_table_name": new_table,
                "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                "dry_run": dry_run,
            }

            try:
                original_card = client.get_card(card_id)
                log_entry["card_name"] = original_card.name
                log_entry["card_type"] = _get_stage_type(original_card) or "unknown"

                migrated_card = migrate_card(
                    card=original_card,
                    field_mapping=field_mapping,
                    table_mapping=table_mapping,
                    column_mapping=column_mapping,
                    old_schema=legacy_schema,
                    new_schema=new_schema,
                    old_table=legacy_table,
                    new_table=new_table,
                )

                if dry_run:
                    _print_diff(original_card, migrated_card, card_id)
                    log_entry["success"] = True
                else:
                    client.put_card(card_id, migrated_card)
                    logger.info("✓ Migrated card %d (%s)", card_id, original_card.name)
                    log_entry["success"] = True

                card_id_str = str(card_id)
                if card_id_str in _dash_dep_cache:
                    linked_dashboards = _dash_dep_cache[card_id_str]
                    for dash_info in linked_dashboards:
                        dash_id = dash_info["id"]
                        dash_name = dash_info["name"]
                        dash_detail = client.fetch_dashboard_details(dash_id)

                        updated_dash = migrate_dashboard(
                            dashboard_detail=dash_detail,
                            card_id=card_id,
                            field_mapping=field_mapping,
                        )
                        if updated_dash != dash_detail:
                            if dry_run:
                                _print_dashboard_diff(
                                    original=dash_detail,
                                    migrated=updated_dash,
                                    dash_id=dash_id,
                                    dash_name=dash_name,
                                    card_id=card_id,
                                )
                            else:
                                response = client.session.put(
                                    f"{client.host}/api/dashboard/{dash_id}", json=updated_dash
                                )
                                response.raise_for_status()
                                logger.info("Reconnected filters for dashboard '%s'", dash_name)

                table_success += 1

            except Exception as e:
                logger.error("✗ Failed to migrate card %d: %s", card_id, e)
                log_entry["success"] = False
                log_entry["error"] = str(e)
                table_failure += 1

            table_logs.append(log_entry)

        # Track table-level results
        if table_failure > 0:
            any_table_failed = True
        global_success += table_success
        global_failure += table_failure

        # --- Log results to BigQuery per table ---
        if not dry_run and table_logs and use_api_discovery:
            bq_client = bigquery.Client(project=PROJECT_NAME)
            _log_to_bigquery(bq_client, table_logs, PROJECT_NAME, INT_METABASE_DATASET)

        logger.info(
            "Table '%s' complete: %d succeeded, %d failed",
            legacy_key,
            table_success,
            table_failure,
        )

    # --- Final summary ---
    mode = "DRY RUN" if dry_run else "MIGRATION"
    logger.info(
        "%s COMPLETE: %d tables processed, %d cards succeeded, %d cards failed",
        mode,
        len(tables_to_migrate.root),
        global_success,
        global_failure,
    )

    if any_table_failed:
        raise typer.Exit(code=1)


def _print_diff(original: Any, migrated: Any, card_id: int) -> None:
    """Print a human-readable diff between original and migrated card."""
    original_data = original.model_dump(by_alias=True)
    migrated_data = migrated.model_dump(by_alias=True)

    print(f"\n{'=' * 60}")
    print(f"Card {card_id}: {original.name}")
    stage_type = _get_stage_type(original) or "unknown"
    print(f"Type: {stage_type}")
    print(f"{'=' * 60}")

    _diff_recursive(original_data, migrated_data, path="")

    print()


def _print_dashboard_diff(
    original: dict[str, Any],
    migrated: dict[str, Any],
    dash_id: int,
    dash_name: str,
    card_id: int,
) -> None:
    """Print a human-readable diff between original and migrated dashboard filters."""
    print(f"\n{'=' * 60}")
    print(f"Dashboard {dash_id}: {dash_name} (Filter Update for Card {card_id})")
    print(f"{'=' * 60}")

    _diff_recursive(original, migrated, path="")
    print()


def _diff_recursive(old: Any, new: Any, path: str) -> None:
    """Recursively compare two structures and print differences."""
    if old == new:
        return

    if isinstance(old, dict) and isinstance(new, dict):
        all_keys = set(old.keys()) | set(new.keys())
        for key in sorted(all_keys):
            _diff_recursive(old.get(key), new.get(key), f"{path}.{key}" if path else key)
    elif isinstance(old, list) and isinstance(new, list):
        if old != new:
            print(f"  {path}:")
            print(f"    - {json.dumps(old, default=str)}")
            print(f"    + {json.dumps(new, default=str)}")
    else:
        print(f"  {path}:")
        print(f"    - {json.dumps(old, default=str)}")
        print(f"    + {json.dumps(new, default=str)}")


def _log_to_bigquery(bq_client: Any, logs: list[dict[str, Any]], project_name: str, dataset: str) -> None:
    """Append migration logs to BigQuery."""

    table_ref = f"{project_name}.{dataset}.migration_log"

    try:
        errors = bq_client.insert_rows_json(table_ref, logs)
        if errors:
            logger.error("Failed to insert logs to BigQuery: %s", errors)
        else:
            logger.info("Logged %d entries to %s", len(logs), table_ref)
    except Exception as e:
        logger.error("Failed to log to BigQuery: %s", e)


if __name__ == "__main__":
    app()
