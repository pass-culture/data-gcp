import os
import requests
import urllib3
import time
import json
import typer
from datetime import datetime
import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from google.cloud import storage
from dms_query import DMS_QUERY
import gcsfs
from utils import get_update_since_param


storage_client = storage.Client()
GCP_PROJECT_ID = os.environ["GCP_PROJECT"]
DATA_GCS_BUCKET_NAME = os.environ["DATA_GCS_BUCKET_NAME"]
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")

API_URL = "https://www.demarches-simplifiees.fr/api/v2/graphql"
demarches_jeunes = [47380, 47480]
demarches_pro = [50362, 55475, 57081, 57189, 61589, 62703, 65028]


def access_secret_data(project_id, secret_id, version_id=2, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


DMS_TOKEN = access_secret_data(GCP_PROJECT_ID, "token_dms")


def run(target, updated_since):

    print("updated_since", updated_since)

    if target == "jeunes":
        fetch_dms_jeunes(updated_since)
        return updated_since

    if target == "pro":
        fetch_dms_pro(updated_since)
        return updated_since


def fetch_dms_jeunes(updated_since):
    result = fetch_result(demarches_jeunes, updated_since=updated_since)
    save_json(
        result,
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/unsorted_dms_jeunes_{updated_since}.json",
    )


def fetch_dms_pro(updated_since):
    result = fetch_result(demarches_pro, updated_since=updated_since)
    save_json(
        result,
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/unsorted_dms_pro_{updated_since}.json",
    )


def fetch_result(demarches_ids, updated_since):
    result = {}
    for demarche_id in demarches_ids:
        end_cursor = ""
        query_body = get_query_body(demarche_id, "", updated_since)
        has_next_page = True
        while has_next_page:
            resultTemp = run_query(query_body)
            for node in resultTemp["data"]["demarche"]["dossiers"]["edges"]:
                dossier = node["node"]
                if dossier is not None:
                    dossier["demarche_id"] = demarche_id
            has_next_page = resultTemp["data"]["demarche"]["dossiers"]["pageInfo"][
                "hasNextPage"
            ]
            result = mergeDictionary(result, resultTemp)
            if ENV_SHORT_NAME != "prod":
                has_next_page = False

            if has_next_page:
                end_cursor = resultTemp["data"]["demarche"]["dossiers"]["pageInfo"][
                    "endCursor"
                ]
                query_body = get_query_body(demarche_id, end_cursor, updated_since)
    if not isinstance(result["data"], list):
        result["data"] = [result["data"]]
    return result


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
        API_URL, json=query_body, headers=headers, verify=False
    )  # warn: SSL verification disabled
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception(
            "Query failed to run by returning code of {}. {}".format(
                request.status_code, query_body
            )
        )


def mergeDictionary(dict_1, dict_2):
    dict_3 = {**dict_1, **dict_2}
    for key, value in dict_3.items():
        if key in dict_1 and key in dict_2:
            if isinstance(dict_1[key], list):
                list_value = [value]
                dict_3[key] = list_value + dict_1[key]
            else:
                dict_3[key] = [value, dict_1[key]]
    return dict_3


def save_json(json_object, filename):
    fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
    with fs.open(filename, "w") as json_file:
        json_file.write(json.dumps(json_object))
    result = filename + " upload complete"
    return {"response": result}


if __name__ == "__main__":
    print("Run DMS !")
    typer.run(run)
