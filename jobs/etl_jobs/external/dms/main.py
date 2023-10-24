import requests
import urllib3
import time
import json
import typer
import pandas as pd
from dms_query import DMS_QUERY
import gcsfs
from utils import API_URL, access_secret_data, demarches_jeunes, demarches_pro


def run(target, updated_since, gcp_project_id, env_short_name):

    print("updated_since", updated_since)

    if target == "jeunes":
        fetch_dms_jeunes(updated_since, env_short_name, gcp_project_id)
        return updated_since

    if target == "pro":
        fetch_dms_pro(updated_since, env_short_name, gcp_project_id)
        return updated_since


def fetch_dms_jeunes(updated_since, env_short_name, gcp_project_id):
    result = fetch_result(
        demarches_jeunes, updated_since, env_short_name, gcp_project_id
    )
    save_json(
        result,
        f"gs://data-bucket-{env_short_name}/dms_export/unsorted_dms_jeunes_{updated_since}.json",
        gcp_project_id,
    )


def fetch_dms_pro(updated_since, env_short_name, gcp_project_id):
    result = fetch_result(demarches_pro, updated_since, env_short_name, gcp_project_id)
    save_json(
        result,
        f"gs://data-bucket-{env_short_name}/dms_export/unsorted_dms_pro_{updated_since}.json",
        gcp_project_id,
    )


def fetch_result(demarches_ids, updated_since, env_short_name, gcp_project_id):
    result = {}
    for demarche_id in demarches_ids:
        end_cursor = ""
        query_body = get_query_body(demarche_id, "", updated_since)
        has_next_page = True
        while has_next_page:
            has_next_page = False
            resultTemp = run_query(query_body, gcp_project_id)
            if "errors" in resultTemp:
                print(resultTemp)
            if resultTemp["data"] is not None:
                for node in resultTemp["data"]["demarche"]["dossiers"]["edges"]:
                    dossier = node["node"]
                    if dossier is not None:
                        dossier["demarche_id"] = demarche_id
                result = mergeDictionary(result, resultTemp)

                has_next_page = resultTemp["data"]["demarche"]["dossiers"]["pageInfo"][
                    "hasNextPage"
                ]
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


def run_query(query_body, gcp_project_id):
    time.sleep(0.2)
    DMS_TOKEN = access_secret_data(gcp_project_id, "token_dms")
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


def save_json(json_object, filename, gcp_project_id):
    fs = gcsfs.GCSFileSystem(project=gcp_project_id)
    with fs.open(filename, "w") as json_file:
        json_file.write(json.dumps(json_object))
    result = filename + " upload complete"
    return {"response": result}


if __name__ == "__main__":
    print("Run DMS !")
    typer.run(run)
