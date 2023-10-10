import pandas as pd
import json
import gcsfs
import logging
import typer
from utils import (
    destination_table_schema_jeunes,
    destination_table_schema_pro,
)

# attention si modif destination_table_schema_jeunes, destination_table_schema_pro dans utils servent a la logique du code
# dans orchestration/dags/jobs/import/import_dms_subscriptions.py ce sont les "vrais schemas" : INT64 au lieu de TIMESTAMP


def parse_api_result(updated_since, dms_target, gcp_project_id, data_gcs_bucket_name):
    logging.info("Start parsing api result")
    logging.info(f"updated_since: {updated_since}")
    logging.info(f"dms_target: {dms_target}")
    if dms_target == "jeunes":
        df_applications = pd.DataFrame(
            columns=[col["name"] for col in destination_table_schema_jeunes]
        )
        fs = gcsfs.GCSFileSystem(project=gcp_project_id)
        with fs.open(
            f"gs://{data_gcs_bucket_name}/dms_export/unsorted_dms_{dms_target}_{updated_since}.json"
        ) as json_file:
            result = json.load(json_file)
        parse_result_jeunes(result, df_applications)
        save_results(
            df_applications,
            dms_target="jeunes",
            updated_since=updated_since,
            data_gcs_bucket_name=data_gcs_bucket_name,
        )
    if dms_target == "pro":
        df_applications = pd.DataFrame(
            columns=[col["name"] for col in destination_table_schema_pro]
        )
        fs = gcsfs.GCSFileSystem(project=gcp_project_id)
        with fs.open(
            f"gs://{data_gcs_bucket_name}/dms_export/unsorted_dms_{dms_target}_{updated_since}.json"
        ) as json_file:
            result = json.load(json_file)
        parse_result_pro(result, df_applications)
        save_results(
            df_applications,
            dms_target="pro",
            updated_since=updated_since,
            data_gcs_bucket_name=data_gcs_bucket_name,
        )
    return


def save_results(df_applications, dms_target, updated_since, data_gcs_bucket_name):
    destination_table_schema = (
        destination_table_schema_jeunes
        if dms_target == "jeunes"
        else destination_table_schema_pro
    )
    for column in destination_table_schema:
        name = column["name"]
        type = column["type"]
        if type == "TIMESTAMP":
            df_applications[f"{name}"] = pd.to_datetime(
                df_applications[f"{name}"], utc=True, origin="unix", errors="coerce"

            )

        elif type == "STRING":
            df_applications[f"{name}"] = df_applications[f"{name}"].astype(str)

    df_applications["update_date"] = pd.to_datetime(
        pd.Timestamp.today(), utc=True, origin="unix"
    )

    df_applications.to_parquet(
        f"gs://{data_gcs_bucket_name}/dms_export/dms_{dms_target}_{updated_since}.parquet",
        engine="pyarrow",
        index=False,
    )
    return


def parse_result_jeunes(result, df_applications):
    for data in result["data"]:
        if data["demarche"]["dossiers"]["edges"] is not None:
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
                        "application_submitted_at": dossier[
                            "datePassageEnConstruction"
                        ],
                        "passed_in_instruction_at": dossier["datePassageEnInstruction"],
                        "processed_at": dossier["dateTraitement"],
                        "application_motivation": dossier["motivation"].replace(
                            "\n", " "
                        )
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


def parse_results_to_table(
    target: str = typer.Option(None, help="pro ou jeunes"),
    updated_since: str = typer.Option(None, help="updated since"),
    bucket_name: str = typer.Option(None, help="data gcs bucket name"),
    project_id: str = typer.Option(None, help="gcp project id"),
):
    parse_api_result(updated_since, target, project_id, bucket_name)
    return


if __name__ == "__main__":
    typer.run(parse_results_to_table)
