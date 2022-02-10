import pandas as pd
import json
import gcsfs
from dependencies.config import (
    DATA_GCS_BUCKET_NAME,
    GCP_PROJECT_ID,
)


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
    df_applications.to_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/dms_{dms_target}_{updated_since}.csv",
        header=False,
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

            if dossier["demandeur"]["siret"]:
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
