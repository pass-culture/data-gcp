import os
import time
from datetime import datetime
import requests
import urllib3
import pandas as pd

from dependencies.access_gcp_secrets import access_secret_data
from dependencies.config import (
    GCP_PROJECT_ID,
    DATA_GCS_BUCKET_NAME,
)


API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
demarches_ids = ["44675", "44623", "29161"]


def parse_result(result, df_applications, demarche_id):
    for node in result["data"]["demarche"]["dossiers"]["edges"]:
        dossier = node["node"]
        dossier_line = {
            "procedure_id": demarche_id,
            "application_id": dossier["id"],
            "application_status": dossier["state"],
            "last_update_at": dossier["dateDerniereModification"],
            "application_submitted_at": dossier["datePassageEnConstruction"],
            "passed_in_instruction_at": dossier["datePassageEnInstruction"],
            "processed_at": dossier["dateTraitement"],
            "instructor_mail": "",
            "applicant_department": "",
            "applicant_birthday": "",
            "applicant_postal_code": "",
        }

        for champ in dossier["champs"]:
            if not champ or "id" not in champ:
                continue
            if champ["id"] == "Q2hhbXAtNTk2NDUz":
                dossier_line["applicant_department"] = champ["stringValue"]
            elif champ["id"] == "Q2hhbXAtNTgyMjIw":
                dossier_line["applicant_birthday"] = champ["stringValue"]
            elif champ["id"] == "Q2hhbXAtNTgyMjIx":
                dossier_line["applicant_postal_code"] = champ["stringValue"]

        for avis in dossier["avis"]:
            if not avis or "instructor_mail" not in avis:
                continue
            dossier_line["instructor_mail"] = avis["profil"]["email"]

        df_applications.loc[len(df_applications)] = dossier_line


def fetch_result(demarches_ids, df_applications, dms_token):
    ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
    for demarche_id in demarches_ids:
        end_cursor = ""
        query_body = get_query_body(demarche_id, "")
        has_next_page = True
        while has_next_page:
            result = run_query(query_body, dms_token)
            parse_result(result, df_applications, demarche_id)

            has_next_page = result["data"]["demarche"]["dossiers"]["pageInfo"][
                "hasNextPage"
            ]

            if ENV_SHORT_NAME != "prod":
                has_next_page = False

            if has_next_page:
                end_cursor = result["data"]["demarche"]["dossiers"]["pageInfo"][
                    "endCursor"
                ]
                query_body = get_query_body(demarche_id, end_cursor)


def get_query_body(demarche_id, end_cursor):
    query = """
        query getDemarches($demarcheNumber: Int!, $after: String) {
            demarche(number:$demarcheNumber) {
                title
                dossiers(first: 100, after: $after) {
                    edges {
                        node {
                            id
                            state
                            dateDerniereModification
                            datePassageEnInstruction
                            datePassageEnConstruction
                            dateTraitement
                            champs {
                                id
                                label
                                stringValue
                            }
                            avis {
                                instructeur {
                                    email
                                }
                            }
                        }
                        cursor
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        }
        """
    variables = {"demarcheNumber": demarche_id, "after": end_cursor}
    query_body = {"query": query, "variables": variables}
    return query_body


def run_query(query_body, dms_token):
    time.sleep(0.2)
    headers = {"Authorization": "Bearer " + dms_token}
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


def save_result(df_applications):
    now = datetime.now()
    df_applications.to_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/dms_{now.year}_{now.month}_{now.day}.csv",
        header=False,
        index=False,
    )


def update_dms_applications():
    df_applications = pd.DataFrame(
        columns=[
            "procedure_id",
            "application_id",
            "application_status",
            "last_update_at",
            "application_submitted_at",
            "passed_in_instruction_at",
            "processed_at",
            "instructor_mail",
            "applicant_department",
            "applicant_birthday",
            "applicant_postal_code",
        ]
    )

    dms_token = access_secret_data(GCP_PROJECT_ID, "token_dms")
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    fetch_result(demarches_ids, df_applications, dms_token)
    save_result(df_applications)
