import requests
import pandas as pd

from utils import BIGQUERY_RAW_DATASET


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
        results += response["result"]["elements"]
        next_page = response["result"].get("nextPage", None)
        i += 1
    df = pd.DataFrame(results)
    df = df.rename(columns=export_columns)
    df["directory_unsubscribe_date"] = pd.to_datetime(df["directory_unsubscribe_date"])
    df[
        [
            "directory_unsubscribe_date",
            "contact_id",
            "email",
            "ext_ref",
        ]
    ].to_gbq(f"{BIGQUERY_RAW_DATASET}.qualtrics_opt_out_users", if_exists="replace")
