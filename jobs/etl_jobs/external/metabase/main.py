import datetime
import json
from pathlib import Path
import logging

import pandas as pd
import typer

from metabase_api import MetabaseAPI
from native import NativeCard
from query import QueryCard
from table import MetabaseTable, get_mapped_fields
from utils import (
    ENVIRONMENT_LONG_NAME,
    ENVIRONMENT_SHORT_NAME,
    INT_METABASE_DATASET,
    METABASE_API_USERNAME,
    PROJECT_NAME,
    access_secret_data,
    get_dependant_cards,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAPPINGS_PATH = Path("data/mappings.json")

METABASE_HOST = access_secret_data(
    PROJECT_NAME, f"metabase_host_{ENVIRONMENT_LONG_NAME}"
)

CLIENT_ID = access_secret_data(
    PROJECT_NAME, f"metabase-{ENVIRONMENT_LONG_NAME}_oauth2_client_id"
)

PASSWORD = access_secret_data(
    PROJECT_NAME, f"metabase-api-secret-{ENVIRONMENT_SHORT_NAME}"
)


def run(
    metabase_card_type: str = typer.Option(
        ...,
        help="Type de update Metabase à faire. 'native' pour les cartes SQL, 'query' pour les cartes en clics boutons, 'dashboard' pour les filtres de dashboards",
    ),
    legacy_table_name: str = typer.Option(
        ...,
        help="Nom de l'ancienne table",
    ),
    new_table_name: str = typer.Option(
        None,
        help="Nom de la nouvelle table",
    ),
    legacy_schema_name: str = typer.Option(
        ...,
        help="Nom de l'ancien schéma Big Query",
    ),
    new_schema_name: str = typer.Option(
        None,
        help="Nom du nouveau schéma Big Query",
    ),
):
    with open(MAPPINGS_PATH, "r") as file:
        data = json.load(file)
        table_columns_mappings = data.get(legacy_table_name, {})

    metabase = MetabaseAPI(
        username=METABASE_API_USERNAME,
        password=PASSWORD,
        host=METABASE_HOST,
        client_id=CLIENT_ID,
    )

    legacy_metabase_table = MetabaseTable(
        legacy_table_name, legacy_schema_name, metabase
    )
    new_metabase_table = MetabaseTable(new_table_name, new_schema_name, metabase)
    legacy_fields_df = legacy_metabase_table.get_fields()
    new_fields_df = new_metabase_table.get_fields()

    legacy_table_id = legacy_metabase_table.get_table_id()
    new_table_id = new_metabase_table.get_table_id()

    metabase_field_mapping = get_mapped_fields(
        legacy_fields_df, new_fields_df, table_columns_mappings
    )
    print(f"Field mapping is {metabase_field_mapping}")

    native_cards, query_cards = get_dependant_cards(
        legacy_table_name, legacy_schema_name
    )

    if metabase_card_type == "native":
        transition_logs = []
        for card_id in native_cards:
            transition_log = {
                "card_type": "native",
                "legacy_table_name": legacy_table_name,
                "new_table_name": new_table_name,
            }
            transition_log["card_id"] = card_id
            transition_log["timestamp"] = datetime.datetime.now()
            try:
                native_card = NativeCard(card_id, metabase)
                native_card.replace_schema_name(
                    legacy_schema_name,
                    new_schema_name,
                    legacy_table_name,
                    new_table_name,
                )
                native_card.replace_table_name(legacy_table_name, new_table_name)
                native_card.replace_column_names(table_columns_mappings)
                native_card.update_filters(metabase_field_mapping)
                native_card.update_query()
                transition_log["success"] = True
            except Exception as e:
                transition_log["success"] = False
                print(e)
            transition_logs.append(transition_log)

    if metabase_card_type == "query":
        transition_logs = []

        for card_id in query_cards:
            transition_log = {
                "card_type": "query",
                "legacy_table_name": legacy_table_name,
                "new_table_name": new_table_name,
            }
            transition_log["card_id"] = card_id
            transition_log["timestamp"] = datetime.datetime.now()
            try:
                query_card = QueryCard(card_id, metabase)
                query_card.update_dataset_query(
                    metabase_field_mapping, legacy_table_id, new_table_id
                )
                query_card.update_table_id(new_table_id)
                query_card.update_card()
                transition_log["success"] = True
            except Exception as e:
                transition_log["success"] = False
                print(e)
            transition_logs.append(transition_log)

    pd.DataFrame(transition_logs).to_gbq(
        (f"{INT_METABASE_DATASET}.migration_log"),
        project_id=PROJECT_NAME,
        if_exists="append",
    )

    return "success"


if __name__ == "__main__":
    typer.run(run)
