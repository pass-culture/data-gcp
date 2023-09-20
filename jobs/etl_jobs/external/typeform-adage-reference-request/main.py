import os
import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from typeform import TypeformDownloader
from gsheet import export_sheet
import json

GCP_PROJECT_ID = os.environ["PROJECT_NAME"]
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def run():

    sheet_df = export_sheet(json.loads(access_secret_data(GCP_PROJECT_ID, SA_ACCOUNT)))
    sheet_df.to_gbq(
        f"{BIGQUERY_RAW_DATASET}.typeform_adage_reference_request_sheet",
        GCP_PROJECT_ID,
        if_exists="replace",
    )

    clean_responses = TypeformDownloader(
        access_secret_data(GCP_PROJECT_ID, "typeform_api_key"), form_id="VtKospEg"
    ).run()
    responses_df = pd.DataFrame(clean_responses)
    responses_df["answers"] = responses_df["answers"].astype(str)
    responses_df = responses_df.drop_duplicates()

    responses_df.to_gbq(
        f"{BIGQUERY_RAW_DATASET}.typeform_adage_reference_request",
        GCP_PROJECT_ID,
        if_exists="replace",
    )
    return "Success"


run()
