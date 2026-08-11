import json
import logging
import re
from collections import defaultdict

import pandas as pd

from core.utils import INT_METABASE_DATASET

logger = logging.getLogger(__name__)


def get_card_lists(metabase):
    cards = metabase.get_cards()

    native_cards = []
    query_cards = []
    for card in cards:
        if card["legacy_query"]:
            card["legacy_query"] = json.loads(card["legacy_query"])
        if card["query_type"] == "native":
            native_cards.append(card)
        elif card["query_type"] == "query":
            query_cards.append(card)

    return native_cards, query_cards


def _extract_table_dependencies(query_attrs, legacy_query):
    table_dependency = []
    query = legacy_query["query"]

    source_table_id = query.get("source-table")
    source_query = query.get("source-query", {})

    if source_table_id is not None:
        table_dependency.append(source_table_id)
    elif "source-table" in source_query:
        table_dependency.append(source_query["source-table"])

    if "joins" in query_attrs:
        for join in query["joins"]:
            table_dependency.append(join["source-table"])

    return table_dependency


def get_query_dependencies(card_list, tables_df):
    monitoring = 0
    dependencies_other = []

    for card in card_list:
        legacy_query = card.get("legacy_query") or {}
        query_block = legacy_query.get("query")
        if not isinstance(query_block, dict):
            monitoring += 1
            continue

        query_attrs = query_block.keys()
        table_dependency = _extract_table_dependencies(query_attrs, legacy_query)

        dependencies_other.append(
            {
                "card_id": card["id"],
                "card_name": card["name"],
                "card_type": card["query_type"],
                "card_owner": card["creator"]["email"],
                "table_id": table_dependency,
            }
        )

    dependencies_other_df = (
        pd.DataFrame(dependencies_other)
        .explode("table_id")
        .reset_index(drop=True)
        .merge(tables_df, how="left", on="table_id", validate="many_to_one")
    )

    logger.info("%d query cards without query legacy", monitoring)

    return dependencies_other_df


def get_table_infos(metabase):
    table_infos = {}
    i = 0
    for table_info in metabase.get_table():
        info = {}
        info["table_id"] = table_info["id"]
        info["table_schema"] = table_info["schema"]
        info["table_name"] = table_info["name"]

        table_infos[i] = info
        i += 1

    tables_df = pd.DataFrame.from_dict(table_infos, orient="index")

    # Deduplicate: multiple Metabase databases can expose the same BQ dataset,
    # causing (table_schema, table_name) duplicates. Keep the lowest table_id
    # (oldest/primary database connection).
    tables_df = tables_df.sort_values("table_id").drop_duplicates(
        subset=["table_schema", "table_name"], keep="first"
    )

    return tables_df


def get_native_dependencies(cards_list, tables_df):
    regex = r"from\s+\w+\.\w+|join\s+\w+\.\w+"

    i = 0
    monitoring = 0
    dependencies_native = {}
    for card in cards_list:
        card_id = card["id"]
        card_name = card["name"]
        card_owner = card["creator"]["email"]
        card_type = card["query_type"]

        legacy_query = card.get("legacy_query")
        native_sql = (legacy_query or {}).get("native", {}).get("query")
        if not native_sql:
            monitoring += 1
            continue
        sql_lines = native_sql.lower().replace("`", "")
        table_dependency = re.findall(regex, sql_lines)
        table_dependency = list(set(table_dependency))

        dependency = {
            "card_id": card_id,
            "card_name": card_name,
            "card_type": card_type,
            "card_owner": card_owner,
            "table_name": [table.split(".")[-1] for table in table_dependency],
        }
        table_schema_list = []
        for dep in table_dependency:
            match_schema = re.search(r"(from|join)\s+(\w+)", dep)
            if match_schema:
                table_schema = match_schema.group(2)
                table_schema_list.append(table_schema)

        dependency["table_schema"] = table_schema_list

        dependencies_native[i] = dependency
        i += 1

    dependencies_native_df = (
        pd.DataFrame.from_dict(dependencies_native, orient="index")
        .explode(["table_name", "table_schema"])
        .reset_index(drop=True)
        .merge(
            tables_df,
            how="left",
            on=["table_schema", "table_name"],
            validate="many_to_one",
        )
    )

    logger.info("%d native cards without query legacy", monitoring)

    return dependencies_native_df


def _audit_duplicate_tables(metabase):
    """Log which query-builder cards reference each duplicate table_id."""
    all_tables = metabase.get_table()
    tables_by_key = defaultdict(list)
    for t in all_tables:
        tables_by_key[(t["schema"], t["name"])].append(
            {"table_id": t["id"], "db_id": t.get("db_id")}
        )

    duplicates = {k: v for k, v in tables_by_key.items() if len(v) > 1}
    if not duplicates:
        logger.info("No duplicate tables found.")
        return

    dup_table_ids = {}
    for (schema, name), entries in duplicates.items():
        for entry in entries:
            dup_table_ids[entry["table_id"]] = {
                "schema": schema,
                "name": name,
                "db_id": entry["db_id"],
            }

    cards = metabase.get_cards()
    card_usage = defaultdict(list)
    for card in cards:
        legacy_query = card.get("legacy_query")
        if legacy_query and isinstance(legacy_query, str):
            try:
                legacy_query = json.loads(legacy_query)
            except Exception:
                legacy_query = None
        if legacy_query and isinstance(legacy_query, dict):
            query_block = legacy_query.get("query", {})
            if isinstance(query_block, dict):
                source_table = query_block.get("source-table")
                if source_table and source_table in dup_table_ids:
                    card_usage[source_table].append(
                        {"card_id": card["id"], "card_name": card["name"]}
                    )
                for join in query_block.get("joins", []):
                    jt = join.get("source-table")
                    if jt and jt in dup_table_ids:
                        card_usage[jt].append(
                            {"card_id": card["id"], "card_name": card["name"]}
                        )

    logger.info("=== DUPLICATE TABLE AUDIT ===")
    for (schema, name), entries in sorted(duplicates.items()):
        logger.info("--- %s.%s ---", schema, name)
        for entry in entries:
            tid = entry["table_id"]
            cards_for_tid = card_usage.get(tid, [])
            logger.info(
                "  db_id=%s  table_id=%s  -> %d card(s)",
                entry["db_id"],
                tid,
                len(cards_for_tid),
            )
            for c in cards_for_tid[:5]:
                logger.info("    card_id=%s  name=%r", c["card_id"], c["card_name"])
            if len(cards_for_tid) > 5:
                logger.info("    ... and %d more", len(cards_for_tid) - 5)
    logger.info("=== END AUDIT ===")


def run_dependencies(metabase):
    _audit_duplicate_tables(metabase)
    tables_df = get_table_infos(metabase)
    native_cards, other_cards = get_card_lists(metabase)
    dependencies_native_df = get_native_dependencies(native_cards, tables_df)
    dependencies_other_df = get_query_dependencies(other_cards, tables_df)

    dependencies_df = pd.concat([dependencies_native_df, dependencies_other_df])

    dependencies_df = dependencies_df.assign(
        card_name=lambda _df: _df.card_name.astype(str),
        card_type=lambda _df: _df.card_type.astype(str),
        table_name=lambda _df: _df.table_name.astype(str),
        card_owner=lambda _df: _df.card_owner.astype(str),
        table_id=lambda _df: _df.table_id.astype(str),
        table_schema=lambda _df: _df.table_schema.astype(str),
    )

    dependencies_df.to_gbq(
        f"{INT_METABASE_DATASET}.card_dependency", if_exists="replace"
    )

    return "success"
