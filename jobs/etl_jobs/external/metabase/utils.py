import os

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
ENVIRONMENT_LONG_NAME = {
    "dev": "development",
    "stg": "staging",
    "prod": "production",
}[ENVIRONMENT_SHORT_NAME]

INT_METABASE_DATASET = f"int_metabase_{ENVIRONMENT_SHORT_NAME}"
METABASE_API_USERNAME = "metabase-data-bot@passculture.app"


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def get_dependant_cards(legacy_table_name, legacy_schema_name):
    query = f"""
        SELECT distinct card_id, card_type
        FROM `{PROJECT_NAME}.{INT_METABASE_DATASET}.card_dependency`
        WHERE table_name = 'global_user'
        and lower(card_name) not like '%archive%'
        and table_schema = '{legacy_schema_name}'
    """

    metabase_activity_query = f"""
        SELECT *
        FROM `{PROJECT_NAME}.{INT_METABASE_DATASET}.activity`
    """

    dependant_cards = pd.read_gbq(query)
    metabase_activity = pd.read_gbq(metabase_activity_query)
    cards_with_traffic = dependant_cards.merge(
        metabase_activity, on="card_id", how="left"
    )

    df = cards_with_traffic[
        [
            "card_id",
            "card_type",
            "card_name",
            "card_collection_id",
            "slug_reduced_level_2",
            "total_users",
            "total_views",
            "nbr_dashboards",
            "last_execution_date",
        ]
    ].sort_values("total_users", ascending=False)

    native_cards = df.query('card_type == "native"')["card_id"].to_list()
    query_cards = df.query('card_type == "query"')["card_id"].to_list()

    return native_cards, query_cards
