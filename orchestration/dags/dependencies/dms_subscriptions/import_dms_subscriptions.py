import pandas as pd
import json
import gcsfs
from common.config import DATA_GCS_BUCKET_NAME, GCP_PROJECT_ID
import logging

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


def parse_api_result(updated_since, dms_target):
    logging.info("Start parsing api result")
    logging.info(f"updated_since: {updated_since}")
    logging.info(f"dms_target: {dms_target}")
    if dms_target == "jeunes":
        df_applications = pd.DataFrame(
            columns=[
                "procedure_id",
                "application_id",
                "application_number",
                "application_archived",
                "application_status",
                "last_update_at",
                "application_submitted_at",
                "passed_in_instruction_at",
                "processed_at",
                "application_motivation",
                "instructors",
                "applicant_department",
                "applicant_postal_code",
            ]
        )
        fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
        with fs.open(
            f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/unsorted_dms_{dms_target}_{updated_since}.json"
        ) as json_file:
            result = json.load(json_file)
        parse_result_jeunes(result, df_applications)
        save_results(df_applications, dms_target="jeunes", updated_since=updated_since)
    if dms_target == "pro":
        df_applications = pd.DataFrame(
            columns=[
                "procedure_id",
                "application_id",
                "application_number",
                "application_archived",
                "application_status",
                "last_update_at",
                "application_submitted_at",
                "passed_in_instruction_at",
                "processed_at",
                "application_motivation",
                "instructors",
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
        )
        fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
        with fs.open(
            f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/unsorted_dms_{dms_target}_{updated_since}.json"
        ) as json_file:
            result = json.load(json_file)
        parse_result_pro(result, df_applications)
        save_results(df_applications, dms_target="pro", updated_since=updated_since)
    return


def save_results(df_applications, dms_target, updated_since):
    destination_table_schema = (
        destination_table_schema_jeunes
        if dms_target == "jeunes"
        else destination_table_schema_pro
    )
    for column in destination_table_schema:
        name = column["name"]
        type = column["type"]
        if type == "TIMESTAMP":
            df_applications[f"{name}"] = pd.to_datetime(df_applications[f"{name}"])
        elif type == "STRING":
            df_applications[f"{name}"] = df_applications[f"{name}"].astype(str)
    df_applications["update_date"] = pd.to_datetime("today")
    df_applications.to_parquet(
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/dms_{dms_target}_{updated_since}.parquet",
        engine="pyarrow",
        index=False,
    )
    return


def parse_result_jeunes(result, df_applications):
    for data in result["data"]:
        for node in data["demarche"]["dossiers"]["edges"]:
            dossier = node["node"]
            if dossier is not None:
                dossier_line = {
                    "procedure_id": dossier["demarche_id"],
                    "application_id": dossier["id"],
                    "application_number": dossier["number"],
                    "application_archived": dossier["archived"],
                    "application_status": dossier["state"],
                    "last_update_at": dossier["dateDerniereModification"],
                    "application_submitted_at": dossier["datePassageEnConstruction"],
                    "passed_in_instruction_at": dossier["datePassageEnInstruction"],
                    "processed_at": dossier["dateTraitement"],
                    "application_motivation": dossier["motivation"].replace("\n", " ")
                    if dossier["motivation"]
                    else None,
                    "instructors": "",
                }
                for champ in dossier["champs"]:
                    if not champ or "id" not in champ:
                        continue
                    if champ["id"] == "Q2hhbXAtNTk2NDUz":
                        dossier_line["applicant_department"] = champ["stringValue"]
                    elif champ["id"] == "Q2hhbXAtNTgyMjIx":
                        dossier_line["applicant_postal_code"] = champ["stringValue"]

                instructeurs = []
                for instructeur in dossier["instructeurs"]:
                    instructeurs.append(instructeur["email"])
                if instructeurs != []:
                    dossier_line["instructors"] = "; ".join(instructeurs)

                df_applications.loc[len(df_applications)] = dossier_line
    return


def parse_result_pro(result, df_applications):
    logging.info(f"Size of json to parse: {len(result['data'])}")
    for data in result["data"]:
        for node in data["demarche"]["dossiers"]["edges"]:
            dossier = node["node"]
            if dossier is not None:
                dossier_line = {
                    "procedure_id": dossier["demarche_id"],
                    "application_id": dossier["id"],
                    "application_number": dossier["number"],
                    "application_archived": dossier["archived"],
                    "application_status": dossier["state"],
                    "last_update_at": dossier["dateDerniereModification"],
                    "application_submitted_at": dossier["datePassageEnConstruction"],
                    "passed_in_instruction_at": dossier["datePassageEnInstruction"],
                    "processed_at": dossier["dateTraitement"],
                    "application_motivation": dossier["motivation"].replace("\n", " ")
                    if dossier["motivation"]
                    else None,
                    "instructors": "",
                }

                if "siret" in dossier["demandeur"] and dossier["demandeur"]["siret"]:
                    dossier_line["demandeur_siret"] = dossier["demandeur"]["siret"]
                    dossier_line["demandeur_naf"] = dossier["demandeur"]["naf"]
                    dossier_line["demandeur_libelleNaf"] = dossier["demandeur"][
                        "libelleNaf"
                    ].replace("\n", " ")
                    if dossier["demandeur"]["entreprise"]:
                        dossier_line["demandeur_entreprise_siren"] = dossier[
                            "demandeur"
                        ]["entreprise"]["siren"]
                        dossier_line["demandeur_entreprise_formeJuridique"] = dossier[
                            "demandeur"
                        ]["entreprise"]["formeJuridique"]
                        dossier_line[
                            "demandeur_entreprise_formeJuridiqueCode"
                        ] = dossier["demandeur"]["entreprise"]["formeJuridiqueCode"]
                        dossier_line[
                            "demandeur_entreprise_codeEffectifEntreprise"
                        ] = dossier["demandeur"]["entreprise"]["codeEffectifEntreprise"]
                        dossier_line["demandeur_entreprise_raisonSociale"] = dossier[
                            "demandeur"
                        ]["entreprise"]["raisonSociale"]
                        dossier_line["demandeur_entreprise_siretSiegeSocial"] = dossier[
                            "demandeur"
                        ]["entreprise"]["siretSiegeSocial"]
                if dossier["champs"]:
                    for champs in dossier["champs"]:
                        if champs["id"] == "Q2hhbXAtMjY3NDMyMQ==":
                            dossier_line["numero_identifiant_lieu"] = champs[
                                "stringValue"
                            ]
                        elif champs["id"] == "Q2hhbXAtMjQzODcyMA==":
                            dossier_line["statut"] = champs["stringValue"]
                        elif champs["id"] == "Q2hhbXAtMjQzMTg1OA==":
                            dossier_line["typologie"] = champs["stringValue"]
                        elif champs["id"] == "Q2hhbXAtMjQzMjIxMg==":
                            dossier_line["academie_historique_intervention"] = champs[
                                "stringValue"
                            ]
                        elif champs["id"] == "Q2hhbXAtMjQzMjM1Mw==":
                            dossier_line["domaines"] = champs["stringValue"]
                else:
                    dossier_line["numero_identifiant_lieu"] = None
                instructeurs = []
                for instructeur in dossier["instructeurs"]:
                    instructeurs.append(instructeur["email"])
                if instructeurs != []:
                    dossier_line["instructors"] = "; ".join(instructeurs)

                if dossier["groupeInstructeur"]:
                    dossier_line["academie_groupe_instructeur"] = dossier[
                        "groupeInstructeur"
                    ]["label"]

                if dossier["annotations"]:
                    for annotation in dossier["annotations"]:
                        if annotation["label"] == "Erreur traitement pass Culture":
                            dossier_line["erreur_traitement_pass_culture"] = annotation[
                                "stringValue"
                            ]
                else:
                    dossier_line["erreur_traitement_pass_culture"] = None

                df_applications.loc[len(df_applications)] = dossier_line
    return


SQL_ANALYTICS_PATH = f"dependencies/dms_subscriptions/sql/analytics"
ANALYTICS_TABLES = {
    "dms_jeunes": {
        "sql": f"{SQL_ANALYTICS_PATH}/dms_deduplicated.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "dms_jeunes",
        "params": {"target": "jeunes"},
    },
    "dms_pro": {
        "sql": f"{SQL_ANALYTICS_PATH}/dms_deduplicated.sql",
        "write_disposition": "WRITE_TRUNCATE",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "dms_pro",
        "params": {"target": "pro"},
    },
}
