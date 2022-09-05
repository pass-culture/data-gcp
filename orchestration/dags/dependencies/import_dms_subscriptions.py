import pandas as pd
import json
import gcsfs
from common.config import (
    DATA_GCS_BUCKET_NAME,
    GCP_PROJECT_ID,
)

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
]


def parse_api_result(updated_since, dms_target):
    print("updated_since:", updated_since)
    print("dms_target:", dms_target)
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
    for data in result["data"]:
        for node in data["demarche"]["dossiers"]["edges"]:
            dossier = node["node"]
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
                    dossier_line["demandeur_entreprise_siren"] = dossier["demandeur"][
                        "entreprise"
                    ]["siren"]
                    dossier_line["demandeur_entreprise_formeJuridique"] = dossier[
                        "demandeur"
                    ]["entreprise"]["formeJuridique"]
                    dossier_line["demandeur_entreprise_formeJuridiqueCode"] = dossier[
                        "demandeur"
                    ]["entreprise"]["formeJuridiqueCode"]
                    dossier_line[
                        "demandeur_entreprise_codeEffectifEntreprise"
                    ] = dossier["demandeur"]["entreprise"]["codeEffectifEntreprise"]
                    dossier_line["demandeur_entreprise_raisonSociale"] = dossier[
                        "demandeur"
                    ]["entreprise"]["raisonSociale"]
                    dossier_line["demandeur_entreprise_siretSiegeSocial"] = dossier[
                        "demandeur"
                    ]["entreprise"]["siretSiegeSocial"]

            instructeurs = []
            for instructeur in dossier["instructeurs"]:
                instructeurs.append(instructeur["email"])
            if instructeurs != []:
                dossier_line["instructors"] = "; ".join(instructeurs)

            df_applications.loc[len(df_applications)] = dossier_line
    return
