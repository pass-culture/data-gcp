import json
import logging

import pandas_gbq
import typer

from catalog import SHEETS
from gcp import BIGQUERY_RAW_DATASET, GCP_PROJECT_ID, SA_ACCOUNT, access_secret_data
from gsheet import export_sheet

logger = logging.getLogger(__name__)


def main():
    try:
        for k, v in SHEETS.items():
            sheet_df = export_sheet(
                json.loads(access_secret_data(GCP_PROJECT_ID, SA_ACCOUNT)), v
            )
            pandas_gbq.to_gbq(
                sheet_df,
                f"{BIGQUERY_RAW_DATASET}.{k}",
                project_id=GCP_PROJECT_ID,
                if_exists="replace",
            )
    except typer.Exit:
        raise
    except Exception as e:
        logger.exception(f"ETL job failed: {e}")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    typer.run(main)
