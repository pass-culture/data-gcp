from datetime import datetime, timedelta

import pandas as pd
import typer

from constants import (
    MACRO_ACTIONS_COLUMNS_BQ_SCHEMA_FIELD,
    TICKET_COLUMN_BQ_SCHEMA_FIELD,
)
from extract import ZendeskAPI
from utils import (
    ZENDESK_API_EMAIL,
    ZENDESK_API_KEY,
    ZENDESK_SUBDOMAIN,
    save_multiple_partitions_to_bq,
    save_to_bq,
)


def main(
    ndays: int = typer.Option(
        28,
        help="Total days to import data from Zendesk.",
    ),
    job: str = typer.Option(
        "both",
        help="Specify the job to run: 'macro_stat', 'ticket_stat', or 'both'.",
    ),
    prior_date: str = typer.Option(
        None,
        help="Optional prior date (YYYY-MM-DD) to calculate the ndays range from instead of now().",
    ),
):
    """
    Main function to import Zendesk data into BigQuery.

    This script retrieves macro usage statistics and/or ticket data from Zendesk
    for the specified number of days and saves them to BigQuery.

    Args:
        ndays (int): Number of days to retrieve ticket data. Defaults to 28.
        job (str): Specify which job to run ('macro_stat', 'ticket_stat', or 'both').
        prior_date (str): Optional prior date (YYYY-MM-DD) to calculate the ndays range.
    """
    # Determine the reference date (either provided prior_date or today)
    reference_date = (
        datetime.strptime(prior_date, "%Y-%m-%d") if prior_date else datetime.now()
    )
    export_date = reference_date.strftime("%Y-%m-%d")

    # Initialize the Zendesk API client
    zendesk_api = ZendeskAPI(
        {
            "subdomain": ZENDESK_SUBDOMAIN,
            "email": ZENDESK_API_EMAIL,
            "token": ZENDESK_API_KEY,
        }
    )

    # Run macro usage statistics job
    if job in ("macro_stat", "both"):
        macro_df = zendesk_api.create_macro_stat_df()
        save_to_bq(
            df=macro_df,
            table_name="zendesk_macro_usage",
            schema_field=MACRO_ACTIONS_COLUMNS_BQ_SCHEMA_FIELD,
            event_date=export_date,
            date_column="export_date",
        )

    # Run ticket statistics job
    if job in ("ticket_stat", "both"):
        from_date = (reference_date - timedelta(days=ndays)).strftime("%Y-%m-%d")
        to_date = prior_date
        ticket_df = zendesk_api.create_ticket_stat_df(
            from_date=from_date, to_date=to_date
        )

        # Add updated and export date columns to the ticket DataFrame
        ticket_df["updated_date"] = pd.to_datetime(ticket_df["updated_at"]).dt.date
        ticket_df["export_date"] = export_date

        # Save the ticket data with partitioning by updated date
        save_multiple_partitions_to_bq(
            df=ticket_df,
            table_name="zendesk_ticket",
            schema_field=TICKET_COLUMN_BQ_SCHEMA_FIELD,
            date_column="updated_date",
        )


if __name__ == "__main__":
    typer.run(main)
