import time
from typing import Annotated

import pandas as pd
import pandas_gbq
import typer
from loguru import logger

from qualtrics_client import QualtricsClient
from qualtrics_export import export_beneficiary_to_qualtrics, export_venue_to_qualtrics
from qualtrics_opt_out import import_qualtrics_opt_out
from qualtrics_survey_answers import process_survey_answers
from schemas import ANSWERS_SCHEMA, OPT_OUT_EXPORT_COLUMNS
from utils import (
    BIGQUERY_RAW_DATASET,
    ENV_SHORT_NAME,
    PROJECT_NAME,
    access_secret_data,
    save_partition_table_to_bq,
)

MAILING_LIST_SECRETS = {
    "export_beneficiary": f"qualtrics_ir_jeunes_automation_id_{ENV_SHORT_NAME}",
    "export_venue": f"qualtrics_ir_ac_automation_id_{ENV_SHORT_NAME}",
}

RATE_LIMIT_SLEEP_S = 60

app = typer.Typer(help="Qualtrics ETL Operations Pipelines", no_args_is_help=True)

_client: QualtricsClient | None = None
_directory_id: str = ""


def _get_client() -> QualtricsClient:
    assert _client is not None, "QualtricsClient not initialized — callback did not run"
    return _client


@app.callback()
def init() -> None:
    global _client, _directory_id
    api_token = access_secret_data(PROJECT_NAME, f"qualtrics_token_{ENV_SHORT_NAME}")
    data_center = access_secret_data(
        PROJECT_NAME, f"qualtrics_data_center_{ENV_SHORT_NAME}"
    )
    _directory_id = access_secret_data(
        PROJECT_NAME, f"qualtrics_directory_id_{ENV_SHORT_NAME}"
    )
    _client = QualtricsClient(api_token=api_token, data_center=data_center)


@app.command(name="import_opt_out_users")
def import_opt_out_users_cmd():
    """Fetch opt-out users from Qualtrics directory and sync to BigQuery."""
    try:
        import_qualtrics_opt_out(_get_client(), _directory_id, OPT_OUT_EXPORT_COLUMNS)
        logger.info("Successfully imported opt-out users.")
    except Exception as e:
        logger.exception(f"import_opt_out_users failed: {e}")
        raise typer.Exit(code=1)


@app.command(name="import_all_survey_answers")
def import_all_survey_answers_cmd():
    """Fetch active survey metadata and sync all answers to BigQuery."""
    try:
        client = _get_client()
        surveys = pd.DataFrame(client.list_surveys())
        pandas_gbq.to_gbq(
            surveys,
            f"{BIGQUERY_RAW_DATASET}.qualtrics_survey",
            project_id=PROJECT_NAME,
            if_exists="replace",
        )
        active_surveys = surveys.loc[lambda df: df.isActive].id.tolist()

        if not active_surveys:
            logger.info("No active surveys found.")
            return

        for i, s_id in enumerate(active_surveys, start=1):
            logger.info(f"[{i}/{len(active_surveys)}] Processing survey {s_id}")
            df = client.download_survey_responses(s_id)
            df = process_survey_answers(df, s_id)
            save_partition_table_to_bq(
                df, "qualtrics_answers", ANSWERS_SCHEMA, "survey_int_id"
            )
            if i % 10 == 0:
                time.sleep(RATE_LIMIT_SLEEP_S)

        logger.info("Successfully processed all survey answers.")
    except Exception as e:
        logger.exception(f"import_all_survey_answers failed: {e}")
        raise typer.Exit(code=1)


@app.command(name="export_beneficiary")
def export_beneficiary_cmd(
    ds: Annotated[str, typer.Option(help="Execution date (YYYY-MM-DD)")] = "",
    dataset_name: Annotated[str, typer.Option(help="BQ source dataset")] = "",
    table_name: Annotated[str, typer.Option(help="BQ source table")] = "",
):
    """Export beneficiary data from BigQuery to Qualtrics mailing list."""
    try:
        mailing_list_id = access_secret_data(
            PROJECT_NAME, MAILING_LIST_SECRETS["export_beneficiary"]
        )
        export_beneficiary_to_qualtrics(
            ds=ds,
            dataset_name=dataset_name,
            table_name=table_name,
            directory_id=_directory_id,
            mailing_list_id=mailing_list_id,
            client=_get_client(),
        )
        logger.info("Successfully exported beneficiaries to Qualtrics.")
    except Exception as e:
        logger.exception(f"export_beneficiary failed: {e}")
        raise typer.Exit(code=1)


@app.command(name="export_venue")
def export_venue_cmd(
    ds: Annotated[str, typer.Option(help="Execution date (YYYY-MM-DD)")] = "",
    dataset_name: Annotated[str, typer.Option(help="BQ source dataset")] = "",
    table_name: Annotated[str, typer.Option(help="BQ source table")] = "",
):
    """Export venue data from BigQuery to Qualtrics mailing list."""
    try:
        mailing_list_id = access_secret_data(
            PROJECT_NAME, MAILING_LIST_SECRETS["export_venue"]
        )
        export_venue_to_qualtrics(
            ds=ds,
            dataset_name=dataset_name,
            table_name=table_name,
            directory_id=_directory_id,
            mailing_list_id=mailing_list_id,
            client=_get_client(),
        )
        logger.info("Successfully exported venues to Qualtrics.")
    except Exception as e:
        logger.exception(f"export_venue failed: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
