"""Typer CLI entry point for the Metabase migration tool.

Provides the `migrate` command with `--dry-run` support.
Orchestrates the full migration flow:
1. Discover impacted cards (via BigQuery)
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

app = typer.Typer(
    name="metabase-migration",
    help="Migrate Metabase cards after BigQuery table/column renames.",
)


@app.callback()
def _callback() -> None:
    """Metabase migration tool."""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MAPPINGS_PATH = Path("data/mappings.json")


@app.command()
def migrate(
    legacy_table_name: str = typer.Option(
        ..., help="Name of the legacy table in BigQuery"
    ),
    new_table_name: str = typer.Option(..., help="Name of the new table in BigQuery"),
    legacy_schema_name: str = typer.Option(
        ..., help="Schema (dataset) of the legacy table"
    ),
    new_schema_name: str = typer.Option(..., help="Schema (dataset) of the new table"),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show diffs without writing changes to Metabase",
    ),
    mappings_file: str = typer.Option(
        str(MAPPINGS_PATH),
        "--mappings-file",
        help="Path to column mappings JSON file",
    ),
    card_ids_str: str = typer.Option(
        "",
        "--card-ids",
        help="Comma-separated card IDs to migrate (skips BigQuery discovery)",
    ),
) -> None:
    """Migrate Metabase cards from a legacy table to a new table.

    Discovers impacted cards via BigQuery, builds field mappings
    by matching column names, and updates each card.

    Supports both native SQL cards and query builder cards in a
    single pass. Use --dry-run to preview changes without writing.
    """
    # Late imports to avoid import errors when just showing --help
    from api.client import MetabaseClient
    from config import (
        METABASE_API_USERNAME,
        get_metabase_host,
        get_metabase_password,
    )
    from discovery.bigquery import (
        build_field_mapping,
        build_table_mapping,
    )
    from migration.card import migrate_card

    # Determine if we're using local card IDs (no BigQuery)
    use_bigquery = not card_ids_str

    # --- Load column mappings ---
    column_mapping: dict[str, str] = {}
    mappings_path = Path(mappings_file)
    if mappings_path.exists():
        with open(mappings_path) as f:
            all_mappings = json.load(f)
            column_mapping = all_mappings.get(legacy_table_name, {})
        logger.info(
            "Loaded %d column mappings for table '%s'",
            len(column_mapping),
            legacy_table_name,
        )
    else:
        logger.warning("No mappings file found at %s", mappings_path)

    # --- Connect to services ---
    logger.info("Connecting to Metabase...")
    metabase_host = get_metabase_host()

    # Authenticate
    bearer_token = None
    if use_bigquery:
        try:
            from google.auth.transport.requests import Request
            from google.oauth2 import id_token

            from config import get_metabase_client_id

            client_id = get_metabase_client_id()
            bearer_token = id_token.fetch_id_token(Request(), client_id)
        except Exception:
            logger.info("GCP OAuth2 not available, using direct auth")

    metabase_password = get_metabase_password()
    client = MetabaseClient.from_credentials(
        host=metabase_host,
        username=METABASE_API_USERNAME,
        password=metabase_password,
        bearer_token=bearer_token,
    )

    # --- Discover impacted cards ---
    bq_client = None
    if use_bigquery:
        from google.cloud import bigquery

        from config import INT_METABASE_DATASET, PROJECT_NAME
        from discovery.bigquery import get_impacted_cards

        logger.info("Discovering impacted cards via BigQuery...")
        bq_client = bigquery.Client(project=PROJECT_NAME)
        card_ids = get_impacted_cards(
            bq_client=bq_client,
            legacy_table=legacy_table_name,
            legacy_schema=legacy_schema_name,
            project_name=PROJECT_NAME,
            dataset=INT_METABASE_DATASET,
        )
    else:
        card_ids = [int(cid.strip()) for cid in card_ids_str.split(",") if cid.strip()]
        logger.info("Using provided card IDs (BigQuery discovery skipped)")

    logger.info("Found %d impacted cards", len(card_ids))

    if not card_ids:
        logger.info("No cards to migrate. Done.")
        return

    # --- Build mappings ---
    logger.info("Building field mappings...")
    legacy_table_id = client.find_table_id(legacy_table_name, legacy_schema_name)
    new_table_id = client.find_table_id(new_table_name, new_schema_name)

    if legacy_table_id is None:
        logger.error(
            "Legacy table '%s.%s' not found in Metabase",
            legacy_schema_name,
            legacy_table_name,
        )
        raise typer.Exit(code=1)

    if new_table_id is None:
        logger.error(
            "New table '%s.%s' not found in Metabase",
            new_schema_name,
            new_table_name,
        )
        raise typer.Exit(code=1)

    field_mapping = build_field_mapping(
        metabase_client=client,
        legacy_table_id=legacy_table_id,
        new_table_id=new_table_id,
        column_mapping=column_mapping,
    )
    table_mapping = build_table_mapping(legacy_table_id, new_table_id)

    logger.info(
        "Mappings: %d fields, %d tables",
        len(field_mapping),
        len(table_mapping),
    )

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
            log_entry["card_type"] = (
                original_card.dataset_query.type
                if original_card.dataset_query
                else "unknown"
            )

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
                logger.info(
                    "✓ Migrated card %d (%s)",
                    card_id,
                    original_card.name,
                )
                log_entry["success"] = True

            success_count += 1

        except Exception as e:
            logger.error("✗ Failed to migrate card %d: %s", card_id, e)
            log_entry["success"] = False
            log_entry["error"] = str(e)
            failure_count += 1

        transition_logs.append(log_entry)

    # --- Log results ---
    if not dry_run and transition_logs and use_bigquery and bq_client is not None:
        from config import INT_METABASE_DATASET, PROJECT_NAME

        _log_to_bigquery(bq_client, transition_logs, PROJECT_NAME, INT_METABASE_DATASET)

    # --- Summary ---
    mode = "DRY RUN" if dry_run else "MIGRATION"
    logger.info(
        "%s COMPLETE: %d succeeded, %d failed, %d total",
        mode,
        success_count,
        failure_count,
        len(card_ids),
    )

    if failure_count > 0:
        raise typer.Exit(code=1)


def _print_diff(
    original: Any,
    migrated: Any,
    card_id: int,
) -> None:
    """Print a human-readable diff between original and migrated card."""
    original_data = original.model_dump(by_alias=True)
    migrated_data = migrated.model_dump(by_alias=True)

    print(f"\n{'=' * 60}")
    print(f"Card {card_id}: {original.name}")
    print(
        f"Type: {original.dataset_query.type if original.dataset_query else 'unknown'}"
    )
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
            _diff_recursive(
                old.get(key),
                new.get(key),
                f"{path}.{key}" if path else key,
            )
    elif isinstance(old, list) and isinstance(new, list):
        if old != new:
            print(f"  {path}:")
            print(f"    - {json.dumps(old, default=str)}")
            print(f"    + {json.dumps(new, default=str)}")
    else:
        print(f"  {path}:")
        print(f"    - {json.dumps(old, default=str)}")
        print(f"    + {json.dumps(new, default=str)}")


def _log_to_bigquery(
    bq_client: Any,
    logs: list[dict[str, Any]],
    project_name: str,
    dataset: str,
) -> None:
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
