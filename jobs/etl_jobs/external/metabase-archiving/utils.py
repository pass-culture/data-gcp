import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
ENVIRONMENT_LONG_NAME = {
    "dev": "development",
    "stg": "staging",
    "prod": "production",
}[ENVIRONMENT_SHORT_NAME]

ANALYTICS_DATASET = f"analytics_{ENVIRONMENT_SHORT_NAME}"
CLEAN_DATASET = f"clean_{ENVIRONMENT_SHORT_NAME}"
INT_METABASE_DATASET = f"int_metabase_{ENVIRONMENT_SHORT_NAME}"
METABASE_API_USERNAME = "metabase-data-bot@passculture.app"

parent_folder_to_archive = ["interne", "operationnel", "adhoc"]
limit_inactivity_in_days = {"interne": 90, "operationnel": 30, "adhoc": 90}
max_cards_to_archive = 50


rules = [
    {
        "rule_id": 1,
        "rule_name": "interne_archiving",
        "rule_description": """
            Règle d'archive de la collection interne : 90 jours d'inactivité ou moins de 5 vues en 6 mois et pas de dashboard associé
            Règle d'alerte de la collection interne : moins de 5 vues en 6 mois et au moins 1 dashboard associé
        """,
        "rule_archiving_sql": f"""
            WHERE
                parent_folder = 'interne'
            AND
                (
                days_since_last_execution >= {limit_inactivity_in_days['interne']}
                OR (total_views_6_months <=  5 and nbr_dashboards = 0)
                )
            AND date(card_creation_date) < date_sub(current_date(), interval 14 day)
            AND clean_slug_reduced_level_2 =! 'secretariat_general'
        """,
        "rule_alerting_sql": """
            WHERE
                parent_folder = 'interne'
            AND total_views_6_months <=  5
            AND nbr_dashboards > 0
        """,
    },
    {
        "rule_id": 2,
        "rule_name": "operationnel_archiving",
        "rule_description": "Règle d'archive de la collection operationnel : 30 jours d'inactivité",
        "rule_archiving_sql": f"""
            WHERE
                parent_folder = 'operationnel'
            AND
                days_since_last_execution >= {limit_inactivity_in_days['operationnel']}
            AND date(card_creation_date) < date_sub(current_date(), interval 14 day)
        """,
    },
    {
        "rule_id": 3,
        "rule_name": "adhoc_archiving",
        "rule_description": "Règle d'archive de la collection adhoc : 90 jours d'inactivité ou moins de 5 vues en 6 mois",
        "rule_archiving_sql": f"""
            WHERE
                parent_folder = 'adhoc'
            AND
                (
                days_since_last_execution >= {limit_inactivity_in_days['adhoc']}
                OR total_views_6_months <= 5
                )
            AND date(card_creation_date) < date_sub(current_date(), interval 14 day)
        """,
    },
]


def access_secret_data(project_id, secret_id, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


METABASE_HOST = access_secret_data(
    PROJECT_NAME, f"metabase_host_{ENVIRONMENT_LONG_NAME}"
)

CLIENT_ID = access_secret_data(
    PROJECT_NAME, f"metabase-{ENVIRONMENT_LONG_NAME}_oauth2_client_id"
)

PASSWORD = access_secret_data(
    PROJECT_NAME, f"metabase-api-secret-{ENVIRONMENT_SHORT_NAME}"
)
