import os

from utils import access_secret_data

# Environment Variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-dev")
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
DMS_TOKEN = access_secret_data(GCP_PROJECT_ID, "token_dms")
API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
DE_BIGQUERY_DATA_EXPORT_BUCKET_NAME = f"de-bigquery-data-export-{ENV_SHORT_NAME}"

# Procedure IDs
demarches = {
    "jeunes": [47380, 47480],
    "pro": [50362, 55475, 57081, 57189, 61589, 62703, 65028, 80264, 81184],
    "reduced": [80264],
}

# Common Schema Columns
BASE_SCHEMA = [
    {"name": "procedure_id", "type": "STRING"},
    {"name": "application_id", "type": "STRING"},
    {"name": "application_number", "type": "STRING"},
    {"name": "application_archived", "type": "STRING"},
    {"name": "application_status", "type": "STRING"},
    {"name": "last_update_at", "type": "TIMESTAMP"},
    {"name": "application_submitted_at", "type": "TIMESTAMP"},
    {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
    {"name": "processed_at", "type": "TIMESTAMP"},
    {"name": "instructors", "type": "STRING"},
]

# Additional Schema Columns for 'jeunes' and 'pro'
fields = [
    "demandeur_siret",
    "demandeur_naf",
    "demandeur_libelleNaf",
    "demandeur_entreprise_siren",
    "demandeur_entreprise_formeJuridique",
    "demandeur_entreprise_formeJuridiqueCode",
    "demandeur_entreprise_codeEffectifEntreprise",
    "demandeur_entreprise_raisonSociale",
    "demandeur_entreprise_siretSiegeSocial",
    "numero_identifiant_lieu",
    "statut",
    "typologie",
    "academie_historique_intervention",
    "academie_groupe_instructeur",
    "domaines",
    "erreur_traitement_pass_culture",
]
EXTRA_SCHEMAS = {
    "jeunes": [
        {"name": "applicant_department", "type": "STRING"},
        {"name": "applicant_postal_code", "type": "STRING"},
    ],
    "pro": [{"name": field, "type": "STRING"} for field in fields],
}


def generate_schema(target: str):
    """
    Generate schema based on the target ('jeunes' or 'pro').
    """
    if target not in EXTRA_SCHEMAS:
        raise ValueError(f"Unsupported schema target: {target}")
    return BASE_SCHEMA + EXTRA_SCHEMAS[target]


# Generate schemas
destination_table_schema_jeunes = generate_schema("jeunes")
destination_table_schema_pro = generate_schema("pro")
