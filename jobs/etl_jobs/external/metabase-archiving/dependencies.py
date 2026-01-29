import json
import re

import pandas as pd
import typer

from metabase_api import MetabaseAPI
from utils import (
    CLIENT_ID,
    INT_METABASE_DATASET,
    METABASE_API_USERNAME,
    METABASE_HOST,
    PASSWORD,
)


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


def get_query_dependencies(card_list, tables_df):
    i = 0
    dependencies_other = {}

    for card in card_list:
        card_id = card["id"]
        card_owner = card["creator"]["email"]
        card_name = card["name"]
        card_type = card["query"]["type"]
        query_attributes_keys = card["query"].keys()
        table_dependency = []

        if "source-table" in query_attributes_keys:
            source_table_id = card["query"]["source-table"]
            if "joins" in query_attributes_keys:
                for join in card["query"]["joins"]:
                    table_dependency.append(join["source-table"])
            table_dependency.append(source_table_id)

        elif (
            "source-query" in query_attributes_keys
            and "source-table" in card["query"]["source-query"].keys()
        ):
            source_table = card["query"]["source-query"]["source-table"]
            if "joins" in query_attributes_keys:
                for join in card["query"]["joins"]:
                    table_dependency.append(join["source-table"])
            table_dependency.append(source_table)

        dependency = dict()
        dependency["card_id"] = card_id
        dependency["card_name"] = card_name
        dependency["card_type"] = card_type
        dependency["card_owner"] = card_owner
        dependency["table_id"] = table_dependency

        dependencies_other[i] = dependency
        i += 1

    dependencies_other_df = (
        pd.DataFrame.from_dict(dependencies_other, orient="index")
        .explode("table_id")
        .reset_index(drop=True)
        .merge(tables_df, how="left", on="table_id")
    )

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
    regex = r"from\s+[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+|join\s+[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+"

    i = 0
    dependencies_native = {}
    for card in cards_list:
        card_id = card["id"]
        card_name = card["name"]
        card_owner = card["creator"]["email"]
        card_type = card["native"]["type"]

        sql_lines = card["native"].lower()
        sql_lines = sql_lines.replace("`", "")
        table_dependency = re.findall(regex, sql_lines)
        table_dependency = list(set(table_dependency))

        dependency = dict()
        dependency["card_id"] = card_id
        dependency["card_name"] = card_name
        dependency["card_type"] = card_type
        dependency["card_owner"] = card_owner
        dependency["table_name"] = [table.split(".")[-1] for table in table_dependency]
        table_schema_list = []
        for dep in table_dependency:
            match_schema = re.search(r"(from|join)\s+(\w+)", dep)
            if match_schema:
                table_schema = match_schema.group(2)
                table_schema_list.append(table_schema)

        dependency["table_schema"] = table_schema_list

        dependencies_native[i] = dependency
        i += 1

    print(pd.DataFrame.from_dict(dependencies_native, orient="index").head())
    dependencies_native_df = (
        pd.DataFrame.from_dict(dependencies_native, orient="index")
        .explode(["table_name", "table_schema"])
        .reset_index(drop=True)
        .merge(tables_df, how="left", on=["table_schema", "table_name"])
    )

    return dependencies_native_df


def run():
    metabase = MetabaseAPI(
        username=METABASE_API_USERNAME,
        password=PASSWORD,
        host=METABASE_HOST,
        client_id=CLIENT_ID,
    )

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


if __name__ == "__main__":
    typer.run(run)
