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

app = typer.Typer(name="metabase-migration", help="Migrate Metabase cards after BigQuery table/column renames.")


@app.callback()
def _callback() -> None:
    """Metabase migration tool."""


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAPPINGS_PATH = Path("data/mappings.json")


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
        from discovery.metabase import _sql_references_table

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
    legacy_table_name: str = typer.Option(..., help="Name of the legacy table in BigQuery"),
    new_table_name: str = typer.Option(..., help="Name of the new table in BigQuery"),
    legacy_schema_name: str = typer.Option(..., help="Schema (dataset) of the legacy table"),
    new_schema_name: str = typer.Option(..., help="Schema (dataset) of the new table"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show diffs without writing changes to Metabase"),
    mappings_file: str = typer.Option(str(MAPPINGS_PATH), "--mappings-file", help="Path to column mappings JSON file"),
    card_ids_str: str = typer.Option(
        "", "--card-ids", help="Comma-separated card IDs to migrate (skips API discovery)"
    ),
    database_name: str = typer.Option(..., help="Name of the Metabase database to search tables in"),
) -> None:
    """Migrate Metabase cards from a legacy table to a new table.

    Discovers impacted cards via the Metabase API, builds field mappings
    by matching column names, and updates each card.

    Supports both native SQL cards and query builder cards in a
    single pass. Use --dry-run to preview changes without writing.
    """
    # Late imports to avoid import errors when just showing --help
    from api.client import DATABASE_TABLES_CACHE_PATH, MetabaseClient
    from api.models import Card
    from config import METABASE_API_USERNAME, get_iap_bearer_token, get_metabase_host, get_metabase_password
    from discovery.mapping import build_field_mapping, build_table_mapping
    from migration.card import _get_stage_type, migrate_card

    # Determine if we're using local card IDs (no API discovery)
    use_api_discovery = not card_ids_str

    # --- Load column mappings ---
    column_mapping: dict[str, str] = {}
    mappings_path = Path(mappings_file)
    if mappings_path.exists():
        with open(mappings_path) as f:
            all_mappings = json.load(f)
            column_mapping = all_mappings.get(legacy_table_name, {})
        logger.info("Loaded %d column mappings for table '%s'", len(column_mapping), legacy_table_name)
    else:
        logger.warning("No mappings file found at %s", mappings_path)

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
        legacy_table_id_for_check = client.find_table_id(legacy_table_name, legacy_schema_name)

        # Filter to only cards that actually reference the legacy table
        impacted_cards = [
            card
            for card in found_cards
            if _card_references_legacy_table(card, legacy_table_name, legacy_schema_name, legacy_table_id_for_check)
        ]
        card_ids = [card.id for card in impacted_cards if card.id is not None]
        logger.info(
            "%d cards impacted out of %d found",
            len(card_ids),
            len(found_cards),
        )
    else:
        from discovery.metabase import build_card_dependency_cache, get_impacted_cards_from_cache

        logger.info("Discovering impacted cards via Metabase API...")
        cache = build_card_dependency_cache(client, table_catalog)
        card_ids = get_impacted_cards_from_cache(legacy_table_name, cache)
        logger.info("Found %d impacted cards", len(card_ids))

    if not card_ids:
        logger.info("No cards to migrate. Done.")
        return

    # --- Build mappings ---
    logger.info("Building field mappings...")
    legacy_table_id = client.find_table_id(legacy_table_name, legacy_schema_name)
    new_table_id = client.find_table_id(new_table_name, new_schema_name)

    if legacy_table_id is None:
        logger.error("Legacy table '%s.%s' not found in Metabase", legacy_schema_name, legacy_table_name)
        raise typer.Exit(code=1)

    if new_table_id is None:
        logger.error("New table '%s.%s' not found in Metabase", new_schema_name, new_table_name)
        raise typer.Exit(code=1)

    field_mapping = build_field_mapping(
        metabase_client=client,
        legacy_table_id=legacy_table_id,
        new_table_id=new_table_id,
        column_mapping=column_mapping,
    )
    table_mapping = build_table_mapping(legacy_table_id, new_table_id)

    logger.info("Mappings: %d fields, %d tables", len(field_mapping), len(table_mapping))

    # --- Migrate cards ---
    transition_logs: list[dict[str, Any]] = []
    success_count = 0
    failure_count = 0

    for card_id in card_ids:
        log_entry: dict[str, Any] = {
            "card_id": card_id,
            "legacy_table_name": legacy_table_name,
            "new_table_name": new_table_name,
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
                old_schema=legacy_schema_name,
                new_schema=new_schema_name,
                old_table=legacy_table_name,
                new_table=new_table_name,
            )

            if dry_run:
                _print_diff(original_card, migrated_card, card_id)
                log_entry["success"] = True
            else:
                client.put_card(card_id, migrated_card)
                logger.info("✓ Migrated card %d (%s)", card_id, original_card.name)
                log_entry["success"] = True

            success_count += 1

        except Exception as e:
            logger.error("✗ Failed to migrate card %d: %s", card_id, e)
            log_entry["success"] = False
            log_entry["error"] = str(e)
            failure_count += 1

        transition_logs.append(log_entry)

    # --- Log results ---
    if not dry_run and transition_logs and use_api_discovery:
        from google.cloud import bigquery

        from config import INT_METABASE_DATASET, PROJECT_NAME

        bq_client = bigquery.Client(project=PROJECT_NAME)
        _log_to_bigquery(bq_client, transition_logs, PROJECT_NAME, INT_METABASE_DATASET)

    # --- Summary ---
    mode = "DRY RUN" if dry_run else "MIGRATION"
    logger.info("%s COMPLETE: %d succeeded, %d failed, %d total", mode, success_count, failure_count, len(card_ids))

    if failure_count > 0:
        raise typer.Exit(code=1)


def _print_diff(original: Any, migrated: Any, card_id: int) -> None:
    """Print a human-readable diff between original and migrated card."""
    from migration.card import _get_stage_type

    original_data = original.model_dump(by_alias=True)
    migrated_data = migrated.model_dump(by_alias=True)

    print(f"\n{'=' * 60}")
    print(f"Card {card_id}: {original.name}")
    stage_type = _get_stage_type(original) or "unknown"
    print(f"Type: {stage_type}")
    print(f"{'=' * 60}")

    _diff_recursive(original_data, migrated_data, path="")

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
