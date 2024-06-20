import typer
import pandas as pd
import datetime
from metabase_api import MetabaseAPI
from table import MetabaseTable, get_mapped_fields
from native import NativeCard
from query import QueryCard
from utils import (
    PROJECT_NAME,
    ENVIRONMENT_SHORT_NAME,
    METABASE_API_USERNAME,
    ANALYTICS_DATASET,
    access_secret_data,
    get_dependant_cards,
)

METABASE_HOST = access_secret_data(
    PROJECT_NAME, f"metabase_host_{ENVIRONMENT_SHORT_NAME}"
)

CLIENT_ID = access_secret_data(
    PROJECT_NAME, f"metabase-{ENVIRONMENT_SHORT_NAME}_oauth2_client_id"
)

PASSWORD = access_secret_data(
    PROJECT_NAME, f"metabase-api-secret-{ENVIRONMENT_SHORT_NAME}"
)


def run(
    metabase_card_type: str = typer.Option(
        ...,
        help="Type de carte Metabase. 'native' pour les cartes SQL et 'query' pour les cartes en clics boutons",
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

    native_cards, query_cards = get_dependant_cards(
        legacy_table_name, legacy_schema_name
    )

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

    metabase_field_mapping = get_mapped_fields(legacy_fields_df, new_fields_df)

    if metabase_card_type == "native":
        transition_logs = []
        transition_log = {
            "card_type": "native",
            "legacy_table_name": legacy_table_name,
            "new_table_name": new_table_name,
        }
        for card_id in native_cards:
            transition_log["card_id"] = card_id
            transition_log["timestamp"] = datetime.datetime.now()
            try:
                native_card = NativeCard(card_id, metabase)
                native_card.replace_table_name(legacy_table_name, new_table_name)
                native_card.update_filters(metabase_field_mapping)
                native_card.update_query()
                transition_log["success"] = True
            except:
                transition_log["success"] = False
            transition_logs.append(transition_log)

    if metabase_card_type == "query":
        transition_logs = []
        transition_log = {
            "card_type": "query",
            "legacy_table_name": legacy_table_name,
            "new_table_name": new_table_name,
        }

        for card_id in query_cards:
            transition_log["card_id"] = card_id
            transition_log["timestamp"] = datetime.datetime.now()
            try:
                query_card = QueryCard(card_id, metabase)
                query_card.update_dataset_query(
                    metabase_field_mapping, legacy_table_id, new_table_id
                )
                query_card.update_result_metadata(metabase_field_mapping)
                query_card.update_table_id(new_table_id)
                query_card.update_viz_settings(metabase_field_mapping)
                query_card.update_card()
                transition_log["success"] = True
            except:
                transition_log["success"] = False
            transition_logs.append(transition_log)

    pd.DataFrame(transition_logs).to_gbq(
        (f"{ANALYTICS_DATASET}.metabase_migration_logs"),
        project_id=PROJECT_NAME,
        if_exists="append",
    )

    return "success"


if __name__ == "__main__":
    typer.run(run)
