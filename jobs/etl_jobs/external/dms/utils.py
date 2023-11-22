from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
demarches_jeunes = [47380, 47480]
demarches_pro = [50362, 55475, 57081, 57189, 61589, 62703, 65028, 80264]

# find and replace string to str
destination_table_schema_jeunes = [
    {"name": "procedure_id", "type": "STRING"},
    {"name": "application_id", "type": "STRING"},
    {"name": "application_number", "type": "STRING"},
    {"name": "application_archived", "type": "STRING"},
    {"name": "application_status", "type": "STRING"},
    {"name": "last_update_at", "type": "TIMESTAMP"},
    {"name": "application_submitted_at", "type": "TIMESTAMP"},
    {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
    {"name": "processed_at", "type": "TIMESTAMP"},
    {"name": "application_motivation", "type": "STRING"},
    {"name": "instructors", "type": "STRING"},
    {"name": "applicant_department", "type": "STRING"},
    {"name": "applicant_postal_code", "type": "STRING"},
]

destination_table_schema_pro = [
    {"name": "procedure_id", "type": "STRING"},
    {"name": "application_id", "type": "STRING"},
    {"name": "application_number", "type": "STRING"},
    {"name": "application_archived", "type": "STRING"},
    {"name": "application_status", "type": "STRING"},
    {"name": "last_update_at", "type": "TIMESTAMP"},
    {"name": "application_submitted_at", "type": "TIMESTAMP"},
    {"name": "passed_in_instruction_at", "type": "TIMESTAMP"},
    {"name": "processed_at", "type": "TIMESTAMP"},
    {"name": "application_motivation", "type": "STRING"},
    {"name": "instructors", "type": "STRING"},
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
]


def access_secret_data(project_id, secret_id, version_id="latest", default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default
