import pandas as pd
import requests
from utils import DATA_CENTER, DIRECTORY_ID, API_TOKEN, BIGQUERY_RAW_DATASET

EXPORT_COLUMNS = {
    "contactId": "contact_id",
    "firstName": "first_name",
    "lastName": "last_name",
    "email": "email",
    "phone": "phone",
    "language": "language",
    "extRef": "ext_ref",
    "directoryUnsubscribed": "directory_unsubscribed",
    "directoryUnsubscribeDate": "directory_unsubscribe_date",
}


def import_qualtrics_opt_out():
    next_page = "https://{0}.qualtrics.com/API/v3/directories/{1}/contacts/optedOutContacts".format(
        DATA_CENTER, DIRECTORY_ID
    )
    headers = {
        "x-api-token": API_TOKEN,
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
    df = df.rename(columns=EXPORT_COLUMNS)
    df["directory_unsubscribe_date"] = pd.to_datetime(df["directory_unsubscribe_date"])
    df[
        [
            "directory_unsubscribe_date",
            "contact_id",
            "email",
            "ext_ref",
        ]
    ].to_gbq(f"{BIGQUERY_RAW_DATASET}.qualtrics_opt_out_users", if_exists="replace")


def run(request):

    import_qualtrics_opt_out()

    return "Success"
