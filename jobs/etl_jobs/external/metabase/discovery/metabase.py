"""Discover card→table dependencies via the Metabase API.

Fetches all cards, matches them to known tables, resolves
collection paths, and caches the result to JSON.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from api.client import CARD_DEPENDENCIES_CACHE_PATH, MetabaseClient
from api.models import CardDependencyInfo, TableDependency

logger = logging.getLogger(__name__)


def build_card_dependency_cache(
    client: MetabaseClient,
    table_catalog: dict[str, list[dict[str, Any]]],
    *,
    cache_path: Path = CARD_DEPENDENCIES_CACHE_PATH,
) -> dict[str, TableDependency]:
    """Fetch all Metabase cards, match each to known tables, persist to JSON.

    For native SQL cards, checks whether the table name appears in the SQL text.
    For query builder cards, matches the ``source-table`` ID against the catalog.

    Args:
        client: Authenticated MetabaseClient.
        table_catalog: Output of ``build_table_catalog()`` — maps
            schema → list of ``{"id": int, "name": str, "active": bool}``.
        cache_path: Where to write the JSON cache.

    Returns:
        A dict mapping table names to their ``TableDependency``.
    """
    # Build lookup structures from the table catalog
    table_id_to_info: dict[int, tuple[str, str]] = {}  # table_id → (table_name, schema)
    all_table_names: list[tuple[str, str, int]] = []  # (table_name, schema, table_id)
    for schema, tables in table_catalog.items():
        for t in tables:
            table_id_to_info[t["id"]] = (t["name"], schema)
            all_table_names.append((t["name"], schema, t["id"]))

    logger.info("Fetching all cards from Metabase API...")
    cards = client.fetch_all_cards()
    logger.info("Fetched %d cards", len(cards))

    cache: dict[str, TableDependency] = {}
    collection_cache: dict[int, tuple[str, str]] = {}

    for card in cards:
        card_id = card.get("id")
        card_name = card.get("name", "")
        card_owner = card.get("creator", {}).get("email", "") if card.get("creator") else ""
        collection_id = card.get("collection_id")

        dataset_query = card.get("dataset_query")
        if not dataset_query:
            continue
        stages = dataset_query.get("stages", [])
        if not stages:
            continue

        stage = stages[0]
        lib_type = stage.get("lib/type", "")

        matched_tables: list[tuple[str, str, int]] = []  # (table_name, schema, table_id)

        if lib_type == "mbql.stage/native":
            card_type = "native"
            sql = stage.get("native", "")
            for table_name, schema, table_id in all_table_names:
                if _sql_references_table(sql, table_name, schema):
                    matched_tables.append((table_name, schema, table_id))
        elif lib_type == "mbql.stage/mbql":
            card_type = "query_builder"
            source_table = stage.get("source-table")
            if source_table is not None and source_table in table_id_to_info:
                tname, tschema = table_id_to_info[source_table]
                matched_tables.append((tname, tschema, source_table))
        else:
            continue

        if not matched_tables:
            continue

        # Resolve collection path (cached across cards)
        path_names, path_ids = client.resolve_collection_path(collection_id, collection_cache)

        info = CardDependencyInfo(
            card_name=card_name,
            card_type=card_type,
            card_owner=card_owner,
            collection_path_names=path_names,
            collection_path_ids=path_ids,
        )

        for table_name, schema, table_id in matched_tables:
            if table_name not in cache:
                cache[table_name] = TableDependency(
                    id=table_id,
                    schema=schema,
                    cards_using_table={},
                )
            cache[table_name].cards_using_table[str(card_id)] = info

    # Write cache to disk
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = {name: dep.model_dump(by_alias=True) for name, dep in cache.items()}
    with open(cache_path, "w") as f:
        json.dump(serialized, f, indent=2)

    logger.info(
        "Built card dependency cache: %d tables, %d total card references → %s",
        len(cache),
        sum(len(dep.cards_using_table) for dep in cache.values()),
        cache_path,
    )
    return cache


def load_card_dependency_cache(
    cache_path: Path = CARD_DEPENDENCIES_CACHE_PATH,
) -> dict[str, TableDependency]:
    """Load a previously-built card dependency cache from JSON.

    Args:
        cache_path: Path to the cache file.

    Returns:
        A dict mapping table names to their ``TableDependency``.

    Raises:
        FileNotFoundError: If the cache file does not exist.
    """
    if not cache_path.exists():
        raise FileNotFoundError(f"Card dependency cache not found at {cache_path}")

    with open(cache_path) as f:
        raw: dict[str, Any] = json.load(f)

    return {name: TableDependency.model_validate(data) for name, data in raw.items()}


def get_impacted_cards_from_cache(
    table_name: str,
    cache: dict[str, TableDependency],
) -> list[int]:
    """Return sorted list of card IDs that depend on a table.

    Args:
        table_name: The table name to look up.
        cache: The card dependency cache.

    Returns:
        A sorted list of card IDs. Empty if the table has no dependents.
    """
    dep = cache.get(table_name)
    if dep is None:
        return []
    return sorted(int(cid) for cid in dep.cards_using_table)


def _sql_references_table(sql: str, table_name: str, schema_name: str) -> bool:
    """Check if SQL references schema.table (schema-aware)."""
    # Fast path: exact string match
    if f"{schema_name}.{table_name}" in sql:
        return True
    if f"`{schema_name}`.`{table_name}`" in sql:
        return True

    # Comprehensive check with regex pattern
    pattern = _create_qualified_table_pattern(schema_name, table_name)
    return bool(pattern.search(sql))


def _create_qualified_table_pattern(schema_name: str, table_name: str) -> re.Pattern:
    """Create regex to match schema.table references only."""
    escaped_schema = re.escape(schema_name)
    escaped_table = re.escape(table_name)

    pattern = re.compile(
        r"([\s(`\"\[,]|^)"  # Group 1: Left boundary (preserved)
        r"`?" + escaped_schema + r"`?"  # Schema with optional backticks
        r"\."  # Literal dot separator
        r"`?" + escaped_table + r"`?"  # Table with optional backticks
        r"(?=[\s,)`\"\]]|$)",  # Right lookahead boundary
        re.IGNORECASE,
    )
    return pattern

