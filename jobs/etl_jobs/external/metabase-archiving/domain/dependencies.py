import json
import logging
import re

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
        if not card["legacy_query"]:
            monitoring += 1
            continue

        query_attrs = card["legacy_query"]["query"].keys()
        table_dependency = _extract_table_dependencies(
            query_attrs, card["legacy_query"]
        )

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

    return pd.DataFrame.from_dict(table_infos, orient="index")


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

        if card["legacy_query"]:
            sql_lines = card["legacy_query"]["native"]["query"].lower()
            sql_lines = sql_lines.replace("`", "")
        else:
            monitoring += 1
            continue
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


def run_dependencies(metabase):
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
