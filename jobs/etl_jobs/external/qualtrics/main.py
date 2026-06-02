import time
from typing import Annotated

import typer

from qualtrics_client import QualtricsClient
from qualtrics_export import export_beneficiary_to_qualtrics, export_venue_to_qualtrics
from qualtrics_opt_out import import_qualtrics_opt_out
from qualtrics_survey_answers import import_survey_metadata, process_survey_answers
from schemas import ANSWERS_SCHEMA, OPT_OUT_EXPORT_COLUMNS
from utils import (
    API_TOKEN,
    DATA_CENTER,
    DIRECTORY_ID,
    ENV_SHORT_NAME,
    PROJECT_NAME,
    access_secret_data,
    save_partition_table_to_bq,
)

MAILING_LIST_SECRETS = {
    "export_beneficiary": f"qualtrics_ir_jeunes_automation_id_{ENV_SHORT_NAME}",
    "export_venue": f"qualtrics_ir_ac_automation_id_{ENV_SHORT_NAME}",
}

ir_surveys_mapping = {
    "GRANT_15_17": "SV_3IdnHqrnsuS17oy",
    "GRANT_18": "SV_cBV3xaZ92BoW5sW",
    "pro": "SV_eOOPuFjgZo1emR8",
}

# Initialize modern Typer App instance
app = typer.Typer(
    help="Qualtrics Data Synchronization Pipeline CLI Tools.",
    no_args_is_help=True
)


@app.command(name="import_opt_out_users")
def import_opt_out_users_cmd():
    """Import opt-out users directory listings from Qualtrics to BigQuery."""
    try:
        client = QualtricsClient(api_token=API_TOKEN, data_center=DATA_CENTER)
        import_qualtrics_opt_out(client, DIRECTORY_ID, OPT_OUT_EXPORT_COLUMNS)
        typer.echo("Successfully executed import_opt_out_users.")
    except Exception as e:
        typer.echo(f"Critical error during opt-out import: {e}", err=True)
        raise typer.Exit(code=1)


@app.command(name="import_all_survey_answers")
def import_all_survey_answers_cmd():
    """Download and mirror response payloads for all active Qualtrics surveys into BigQuery."""
    try:
        active_surveys = import_survey_metadata(
            data_center=DATA_CENTER, api_token=API_TOKEN
        )
        
        if not active_surveys:
            typer.echo("No active surveys located for collection processing.")
            return

        client = QualtricsClient(api_token=API_TOKEN, data_center=DATA_CENTER)
        for i, s_id in enumerate(active_surveys, start=1):
            df = client.download_survey_responses(s_id)
            df = process_survey_answers(df, s_id)
            save_partition_table_to_bq(
                df, "qualtrics_answers", ANSWERS_SCHEMA, "survey_int_id"
            )
            if i % 10 == 0:
                time.sleep(60)
                
        typer.echo("Successfully completed all survey answers ingest actions.")
    except Exception as e:
        typer.echo(f"Critical error during active answer aggregation: {e}", err=True)
        raise typer.Exit(code=1)


@app.command(name="export_beneficiary")
def export_beneficiary_cmd(
    ds: Annotated[str, typer.Option(help="Execution target parsing window date (YYYY-MM-DD)")] = "",
    dataset_name: Annotated[str, typer.Option(help="BigQuery target distribution source dataset identity reference")] = "",
    table_name: Annotated[str, typer.Option(help="BigQuery target data query source table reference")] = "",
):
    """Sync beneficiary demographics records out from structured data warehouses directly to active mailing contacts registries."""
    try:
        mailing_list_id = access_secret_data(
            PROJECT_NAME, MAILING_LIST_SECRETS["export_beneficiary"]
        )
        client = QualtricsClient(api_token=API_TOKEN, data_center=DATA_CENTER)
        export_beneficiary_to_qualtrics(
            ds=ds,
            dataset_name=dataset_name,
            table_name=table_name,
            directory_id=DIRECTORY_ID,
            mailing_list_id=mailing_list_id,
            client=client,
        )
        typer.echo("Successfully finalized validation distributions tracking loops out to target listings.")
    except Exception as e:
        typer.echo(f"Critical execution error tracking export distributions maps: {e}", err=True)
        raise typer.Exit(code=1)


@app.command(name="export_venue")
def export_venue_cmd(
    ds: Annotated[str, typer.Option(help="Execution target parsing window date (YYYY-MM-DD)")] = "",
    dataset_name: Annotated[str, typer.Option(help="BigQuery target distribution source dataset identity reference")] = "",
    table_name: Annotated[str, typer.Option(help="BigQuery target data query source table reference")] = "",
):
    """Sync venue tracking demographics records out from data warehouses directly to targeting distribution tables."""
    try:
        mailing_list_id = access_secret_data(
            PROJECT_NAME, MAILING_LIST_SECRETS["export_venue"]
        )
        client = QualtricsClient(api_token=API_TOKEN, data_center=DATA_CENTER)
        export_venue_to_qualtrics(
            ds=ds,
            dataset_name=dataset_name,
            table_name=table_name,
            directory_id=DIRECTORY_ID,
            mailing_list_id=mailing_list_id,
            client=client,
        )
        typer.echo("Successfully finalized venue target listing pipelines metrics mapping.")
    except Exception as e:
        typer.echo(f"Critical validation failures tracing venue transformations tracking sequences: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()