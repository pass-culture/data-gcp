"""Staging Metabase exploration script (read-only).

Connects to the staging Metabase, lists all tables, cross-references with
data/mappings.json, and reports which cards reference each mapped table.

Usage:
    just stg-explore
    # or directly:
    PROJECT_NAME=passculture-data-ehp ENV_SHORT_NAME=stg uv run python scripts/stg_explore.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path so that `api`, `config`, etc. are importable
# when running this script directly (e.g., `python scripts/stg_explore.py`).
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MAPPINGS_PATH = Path(__file__).resolve().parent.parent / "data" / "mappings.json"


def main() -> None:
    from api.client import MetabaseClient
    from config import (
        METABASE_API_USERNAME,
        get_iap_bearer_token,
        get_metabase_host,
        get_metabase_password,
    )

    # --- Load mappings ---
    if not MAPPINGS_PATH.exists():
        logger.error("mappings.json not found at %s", MAPPINGS_PATH)
        sys.exit(1)

    with open(MAPPINGS_PATH) as f:
        all_mappings: dict[str, dict[str, str]] = json.load(f)

    mapped_table_names = set(all_mappings.keys())
    logger.info("Loaded %d tables from mappings.json", len(mapped_table_names))

    # --- Authenticate ---
    metabase_host = get_metabase_host()
    metabase_password = get_metabase_password()
    bearer_token = get_iap_bearer_token()
    client = MetabaseClient.from_credentials(
        host=metabase_host,
        username=METABASE_API_USERNAME,
        password=metabase_password,
        bearer_token=bearer_token,
    )

    # --- List all tables and find matches ---
    logger.info("Fetching Metabase table catalog...")
    all_tables = client.list_tables()

    matched_tables = []
    for t in all_tables:
        name = t.get("name", "")
        if name in mapped_table_names:
            matched_tables.append(t)

    logger.info(
        "Found %d / %d mapped tables in Metabase catalog",
        len(matched_tables),
        len(mapped_table_names),
    )

    if not matched_tables:
        print("\nNo tables from mappings.json found in staging Metabase.")
        return

    # --- Fetch all cards ---
    logger.info("Fetching all cards (this may take a moment)...")
    response = client.session.get(f"{client.host}/api/card")
    response.raise_for_status()
    all_cards = response.json()
    logger.info("Fetched %d cards", len(all_cards))

    # --- Build report ---
    print("\n=== Staging Metabase Impact Report ===\n")

    matched_tables.sort(key=lambda t: t.get("name", ""))

    for table in matched_tables:
        table_name = table["name"]
        table_id = table["id"]
        schema = table.get("schema", "unknown")
        num_renames = len(all_mappings.get(table_name, {}))

        # Find cards referencing this table
        native_cards = []
        qb_cards = []
        for card in all_cards:
            dq = card.get("dataset_query", {})
            if not dq:
                continue
            if dq.get("type") == "native":
                # Check if the table name appears in the native query
                native_query = dq.get("native", {}).get("query", "")
                if table_name in native_query:
                    native_cards.append(card["id"])
            elif dq.get("type") == "query":
                source_table = dq.get("query", {}).get("source-table")
                if source_table == table_id:
                    qb_cards.append(card["id"])

        total_cards = len(native_cards) + len(qb_cards)
        print(f"{table_name} (schema: {schema}, table_id: {table_id})")
        print(f"  {num_renames} column renames in mappings.json")
        print(f"  {total_cards} cards reference this table")
        if native_cards:
            ids_str = ", ".join(str(c) for c in native_cards[:10])
            suffix = (
                f", ... (+{len(native_cards) - 10} more)"
                if len(native_cards) > 10
                else ""
            )
            print(f"  Native SQL cards: {ids_str}{suffix}")
        if qb_cards:
            ids_str = ", ".join(str(c) for c in qb_cards[:10])
            suffix = f", ... (+{len(qb_cards) - 10} more)" if len(qb_cards) > 10 else ""
            print(f"  Query builder cards: {ids_str}{suffix}")
        if not native_cards and not qb_cards:
            print("  (no cards found)")
        print()


if __name__ == "__main__":
    main()
