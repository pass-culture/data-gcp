import requests
import os
import json
import time
import urllib3
import pandas as pd

from google.cloud import secretmanager
from config import DATA_GCS_BUCKET_NAME
from datetime import datetime


API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
demarches_ids = ["44675", "44623", "29161"]
df = pd.DataFrame(
    columns=[
        "demarche_id",
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


def parse_result(result, df, demarche_id):
    for node in result["data"]["demarche"]["dossiers"]["edges"]:
        dossier = node["node"]
        dossier_line = {
            "demarche_id": demarche_id,
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

        df.loc[len(df)] = dossier_line


def fetch_result(demarches_ids, df, dms_token):
    for demarche_id in demarches_ids:
        end_cursor = ""
        query = get_query(demarche_id, "")
        has_next_page = True
        while has_next_page:
            result = run_query(query, dms_token)
            parse_result(result, df, demarche_id)

            has_next_page = result["data"]["demarche"]["dossiers"]["pageInfo"][
                "hasNextPage"
            ]
            has_next_page = False
            # if has_next_page:
            #     end_cursor = result["data"]["demarche"]["dossiers"]["pageInfo"][
            #         "endCursor"
            #     ]
            #     query = get_query(demarche_id, end_cursor)


def get_query(demarche_id, end_cursor):
    if not end_cursor:
        parameter = "first:100"
    else:
        parameter = f'after: "{end_cursor}"'
    query = (
        """
        query getDemarches {
          demarche(number: """
        + demarche_id
        + """) {
            title
            dossiers("""
        + parameter
        + """) {
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
    )
    return query


def run_query(query, dms_token):
    time.sleep(0.2)
    headers = {"Authorization": "Bearer " + dms_token}
    request = requests.post(
        API_URL, json={"query": query}, headers=headers, verify=False
    )  # warn: SSL verification disabled
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(
            "Query failed to run by returning code of {}. {}".format(
                request.status_code, query
            )
        )


def get_secret_token():
    client = secretmanager.SecretManagerServiceClient()
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
    name = f"projects/{GCP_PROJECT_ID}/secrets/token_dms/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def save_result(df):
    # analytics_dataset = os.environ.get("BIGQUERY_ANALYTICS_DATASET", "")
    # target_table = f"{analytics_dataset}.dms_applications"
    # project_id = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")

    df.last_update_at = pd.to_datetime(df.last_update_at)
    df.application_submitted_at = pd.to_datetime(df.application_submitted_at)
    df.passed_in_instruction_at = pd.to_datetime(df.passed_in_instruction_at)
    df.processed_at = pd.to_datetime(df.processed_at)

    now = datetime.now()
    df.to_csv(f"{DATA_GCS_BUCKET_NAME}/dms_export/dms_{now.year}_{now.month}_{now.day}.csv")
    # df.to_gbq(
    #     target_table,
    #     project_id=project_id,
    #     if_exists="replace",
    #     table_schema=[
    #         {"name": "demarche_id", "type": "STRING"},
    #         {"name": "application_id", "type": "STRING"},
    #         {"name": "application_status", "type": "STRING"},
    #         {"name": "last_update_at", "type": "DATETIME"},
    #         {"name": "application_submitted_at", "type": "DATETIME"},
    #         {"name": "passed_in_instruction_at", "type": "DATETIME"},
    #         {"name": "processed_at", "type": "DATETIME"},
    #         {"name": "instructor_mail", "type": "STRING"},
    #         {"name": "applicant_department", "type": "STRING"},
    #         {"name": "applicant_birthday", "type": "STRING"},
    #         {"name": "applicant_postal_code", "type": "STRING"},
    #     ],
    # )


def update_dms_applications():
    dms_token = get_secret_token()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    fetch_result(demarches_ids, df, dms_token)
    save_result(df)
