import os

from utils import access_secret_data

# Environment Variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "passculture-data-dev")
ENV_SHORT_NAME = os.getenv("ENV_SHORT_NAME", "dev")
DMS_TOKEN = access_secret_data(GCP_PROJECT_ID, "token_dms")
API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"

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
EXTRA_SCHEMAS = {
    "jeunes": [
        {"name": "applicant_department", "type": "STRING"},
        {"name": "applicant_postal_code", "type": "STRING"},
    ],
    "pro": [
        {"name": "demandeur_siret", "type": "STRING"},
        {"name": "demandeur_naf", "type": "STRING"},
        {"name": "demandeur_libelleNaf", "type": "STRING"},
        {"name": "demandeur_entreprise_siren", "type": "STRING"},
        {"name": "demandeur_entreprise_formeJuridique", "type": "STRING"},
        {"name": "demandeur_entreprise_formeJuridiqueCode", "type": "STRING"},
        {"name": "demandeur_entreprise_codeEffectifEntreprise", "type": "STRING"},
        {"name": "demandeur_entreprise_raisonSociale", "type": "STRING"},
        {"name": "demandeur_entreprise_siretSiegeSocial", "type": "STRING"},
        {"name": "numero_identifiant_lieu", "type": "STRING"},
        {"name": "statut", "type": "STRING"},
        {"name": "typologie", "type": "STRING"},
        {"name": "academie_historique_intervention", "type": "STRING"},
        {"name": "academie_groupe_instructeur", "type": "STRING"},
        {"name": "domaines", "type": "STRING"},
        {"name": "erreur_traitement_pass_culture", "type": "STRING"},
    ],
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
