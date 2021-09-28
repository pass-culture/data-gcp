import os
import requests
import urllib3
import time
from datetime import datetime
import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

from dms_query import DMS_QUERY


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


GCP_PROJECT_ID = os.environ["GCP_PROJECT"]
DATA_GCS_BUCKET_NAME = os.environ["DATA_GCS_BUCKET_NAME"]
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")

DMS_TOKEN = access_secret_data(GCP_PROJECT_ID, "token_dms")


API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
demarches_jeunes = [44675, 44623, 29161]
demarches_pro = [29425, 29426, 11990]


def run(request):
    """The Cloud Function entrypoint.
    Args:
        request (flask.Request): The request object.
    """

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and "updated_since" in request_json:
        updated_since = request_json["updated_since"]
    elif request_args and "updated_since" in request_args:
        updated_since = request_args["updated_since"]
    else:
        raise RuntimeError("You need to provide an updated_since argument.")

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    print("updated_since", updated_since)
    fetch_dms_jeunes(updated_since)

    fetch_dms_pro(updated_since)

    return updated_since


def fetch_dms_jeunes(updated_since):
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
    fetch_result(
        demarches_jeunes,
        df_applications,
        dms_target="jeunes",
        updated_since=updated_since,
    )
    save_results(df_applications, dms_target="jeunes", updated_since=updated_since)


def fetch_dms_pro(updated_since):
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
    fetch_result(
        demarches_pro, df_applications, dms_target="pro", updated_since=updated_since
    )
    save_results(df_applications, dms_target="pro", updated_since=updated_since)


def fetch_result(demarches_ids, df_applications, dms_target, updated_since):
    for demarche_id in demarches_ids:
        end_cursor = ""
        query_body = get_query_body(demarche_id, "", updated_since)
        has_next_page = True
        while has_next_page:
            result = run_query(query_body)
            print(result)
            parse_result(result, df_applications, demarche_id, dms_target=dms_target)

            has_next_page = result["data"]["demarche"]["dossiers"]["pageInfo"][
                "hasNextPage"
            ]

            if ENV_SHORT_NAME != "prod":
                has_next_page = False

            if has_next_page:
                end_cursor = result["data"]["demarche"]["dossiers"]["pageInfo"][
                    "endCursor"
                ]
                query_body = get_query_body(demarche_id, end_cursor, updated_since)


def get_query_body(demarche_id, end_cursor, updated_since):
    variables = {
        "demarcheNumber": demarche_id,
        "after": end_cursor,
        "updatedSince": updated_since,
    }
    query_body = {"query": DMS_QUERY, "variables": variables}
    return query_body


def run_query(query_body):
    time.sleep(0.2)
    headers = {"Authorization": "Bearer " + DMS_TOKEN}
    request = requests.post(
        API_URL,
        json=query_body,
        headers=headers,
        verify=False,
    )  # warn: SSL verification disabled
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(
            "Query failed to run by returning code of {}. {}".format(
                request.status_code, query_body
            )
        )


def parse_result(result, df_applications, demarche_id, dms_target):
    if dms_target == "pro":
        parse_result_pro(result, df_applications, demarche_id)
    else:
        parse_result_jeunes(result, df_applications, demarche_id)


def parse_result_jeunes(result, df_applications, demarche_id):
    for node in result["data"]["demarche"]["dossiers"]["edges"]:
        dossier = node["node"]
        dossier_line = {
            "procedure_id": demarche_id,
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


def parse_result_pro(result, df_applications, demarche_id):
    for node in result["data"]["demarche"]["dossiers"]["edges"]:
        dossier = node["node"]
        dossier_line = {
            "procedure_id": demarche_id,
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
                dossier_line["demandeur_entreprise_codeEffectifEntreprise"] = dossier[
                    "demandeur"
                ]["entreprise"]["codeEffectifEntreprise"]
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


def save_results(df_applications, dms_target, updated_since):
    df_applications.to_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/dms_{dms_target}_{updated_since}.csv",
        header=False,
        index=False,
    )
