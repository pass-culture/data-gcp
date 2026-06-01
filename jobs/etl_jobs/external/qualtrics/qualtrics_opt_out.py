import pandas as pd
import pandas_gbq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils import BIGQUERY_RAW_DATASET, PROJECT_NAME

_RETRY = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])


def import_qualtrics_opt_out(data_center, directory_id, api_token, export_columns):
    next_page = "https://{0}.qualtrics.com/API/v3/directories/{1}/contacts/optedOutContacts".format(
        data_center, directory_id
    )
    headers = {
        "x-api-token": api_token,
    }
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=_RETRY))

    results = []
    i = 0
    while next_page is not None:
        print(f"Page {i}")
        response = session.get(next_page, headers=headers).json()
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
