import pandas as pd
import pandas_gbq
import requests

from utils import BIGQUERY_RAW_DATASET, PROJECT_NAME


def import_qualtrics_opt_out(data_center, directory_id, api_token, export_columns):
    next_page = "https://{0}.qualtrics.com/API/v3/directories/{1}/contacts/optedOutContacts".format(
        data_center, directory_id
    )
    headers = {
        "x-api-token": api_token,
    }
    results = []
    i = 0
    while next_page is not None:
        print(f"Page {i}")
        response = requests.get(next_page, headers=headers).json()
        if "result" not in response:
            raise RuntimeError(f"Unexpected API response: {response}")
        results += response["result"]["elements"]
        next_page = response["result"].get("nextPage", None)
        i += 1
    df = pd.DataFrame(results)
    df = df.rename(columns=export_columns)
    df["directory_unsubscribe_date"] = pd.to_datetime(df["directory_unsubscribe_date"])
    df_to_load = df[
        [
            "directory_unsubscribe_date",
            "contact_id",
            "email",
            "ext_ref",
        ]
    ]
    pandas_gbq.to_gbq(
        df_to_load,
        f"{BIGQUERY_RAW_DATASET}.qualtrics_opt_out_users",
        project_id=PROJECT_NAME,
        if_exists="replace",
    )
