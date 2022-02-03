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
demarches_jeunes = [44675, 44623, 29161, 47380, 47480]
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
    result_dict = fetch_result(
        demarches_jeunes,
        updated_since=updated_since,
    )
    for index, (key, value) in enumerate(result_dict.items()):
        if index == 0:
            struct_df = pd.DataFrame(value)
            struct_df["demarcheID"] = key
        else:
            temp_df = pd.DataFrame(value)
            temp_df["demarcheID"] = key
            struct_df = pd.concat([struct_df, temp_df], ignore_index=True)
    save_results(struct_df, dms_target="jeunes", updated_since=updated_since)


def fetch_dms_pro(updated_since):
    result_dict = fetch_result(demarches_pro, updated_since=updated_since)
    for index, (key, value) in enumerate(result_dict.items()):
        if index == 0:
            struct_df = pd.DataFrame(value)
            struct_df["demarcheID"] = key
        else:
            temp_df = pd.DataFrame(value)
            temp_df["demarcheID"] = key
            struct_df = pd.concat([struct_df, temp_df], ignore_index=True)
    save_results(struct_df, dms_target="pro", updated_since=updated_since)


def fetch_result(demarches_ids, updated_since):
    result_dict = {}
    for demarche_id in demarches_ids:
        result_list = []
        end_cursor = ""
        query_body = get_query_body(demarche_id, "", updated_since)
        has_next_page = True
        while has_next_page:
            result = run_query(query_body)
            print(result)
            result_list.append(result)
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
        result_dict[f"demarche_id"] = result_list
    return result_dict


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


def save_results(df_applications, dms_target, updated_since):
    df_applications.to_csv(
        f"gs://{DATA_GCS_BUCKET_NAME}/dms_export/unsorted_dms_{dms_target}_{updated_since}.csv",
        header=False,
        index=False,
    )
