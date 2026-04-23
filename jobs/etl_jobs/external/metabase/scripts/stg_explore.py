"""Staging Metabase exploration script (read-only).

Connects to the staging Metabase, builds a card dependency cache via the API,
cross-references with data/mappings.json, and reports which cards reference
each mapped table.

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAPPINGS_PATH = Path(__file__).resolve().parent.parent / "data" / "mappings.json"


def main() -> None:
    import os

    from api.client import DATABASE_TABLES_CACHE_PATH, MetabaseClient
    from config import METABASE_API_USERNAME, get_iap_bearer_token, get_metabase_host, get_metabase_password
    from discovery.metabase import build_card_dependency_cache

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
        host=metabase_host, username=METABASE_API_USERNAME, password=metabase_password, bearer_token=bearer_token
    )

    # --- Build table catalog ---
    logger.info("Fetching Metabase table catalog...")
    if DATABASE_TABLES_CACHE_PATH.exists():
        DATABASE_TABLES_CACHE_PATH.unlink()

    # Use database name from env or default to "BigQuery"
    database_name = os.getenv("DATABASE_NAME", "BigQuery")
    database_id = client.find_database_id(database_name)
    if database_id is None:
        logger.error("Could not find database '%s' in Metabase", database_name)
        sys.exit(1)

    table_catalog = client.build_table_catalog(database_id)

    # --- Build card dependency cache ---
    logger.info("Building card dependency cache via Metabase API...")
    cache = build_card_dependency_cache(client, table_catalog)

    # --- Build report ---
    print("\n=== Staging Metabase Impact Report ===\n")

    for table_name in sorted(cache.keys()):
        if table_name not in mapped_table_names:
            continue

        dep = cache[table_name]
        num_renames = len(all_mappings.get(table_name, {}))

        native_cards = [cid for cid, info in dep.cards_using_table.items() if info.card_type == "native"]
        qb_cards = [cid for cid, info in dep.cards_using_table.items() if info.card_type == "query_builder"]

        total_cards = len(native_cards) + len(qb_cards)
        print(f"{table_name} (schema: {dep.schema_}, table_id: {dep.id})")
        print(f"  {num_renames} column renames in mappings.json")
        print(f"  {total_cards} cards reference this table")
        if native_cards:
            ids_str = ", ".join(native_cards[:10])
            suffix = f", ... (+{len(native_cards) - 10} more)" if len(native_cards) > 10 else ""
            print(f"  Native SQL cards: {ids_str}{suffix}")
        if qb_cards:
            ids_str = ", ".join(qb_cards[:10])
            suffix = f", ... (+{len(qb_cards) - 10} more)" if len(qb_cards) > 10 else ""
            print(f"  Query builder cards: {ids_str}{suffix}")
        if not native_cards and not qb_cards:
            print("  (no cards found)")
        print()


if __name__ == "__main__":
    main()
