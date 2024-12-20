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
):
    """
    Main function to import Zendesk data into BigQuery.

    This script retrieves macro usage statistics and ticket data from Zendesk
    for the specified number of days and saves them to BigQuery.

    Args:
        ndays (int): Number of days to retrieve ticket data. Defaults to 28.
    """
    # Define the export date as today's date
    export_date = datetime.now().strftime("%Y-%m-%d")

    # Initialize the Zendesk API client
    zendesk_api = ZendeskAPI(
        {
            "subdomain": ZENDESK_SUBDOMAIN,
            "email": ZENDESK_API_EMAIL,
            "token": ZENDESK_API_KEY,
        }
    )

    # Retrieve and save macro usage statistics
    macro_df = zendesk_api.create_macro_stat_df()
    save_to_bq(
        df=macro_df,
        table_name="zendesk_macro_usage",
        schema_field=MACRO_ACTIONS_COLUMNS_BQ_SCHEMA_FIELD,
        event_date=export_date,
        date_column="export_date",
    )

    # Retrieve ticket statistics for the specified date range
    updated_at = (datetime.now() - timedelta(days=ndays)).strftime("%Y-%m-%d")
    ticket_df = zendesk_api.create_ticket_stat_df(updated_at=updated_at)

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
