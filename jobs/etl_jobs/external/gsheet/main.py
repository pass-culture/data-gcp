import json
import os

import typer
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

from gsheet import SHEETS, export_sheet

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


def main():
    for k, v in SHEETS.items():
        sheet_df = export_sheet(
            json.loads(access_secret_data(GCP_PROJECT_ID, SA_ACCOUNT)), v
        )
        sheet_df.to_gbq(
            f"{BIGQUERY_RAW_DATASET}.{k}",
            GCP_PROJECT_ID,
            if_exists="replace",
        )


if __name__ == "__main__":
    typer.run(main)
