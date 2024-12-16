from datetime import datetime, timedelta

import typer

from extract import ZendeskAPI
from utils import (
    ZENDESK_API_EMAIL,
    ZENDESK_API_KEY,
    ZENDESK_SUBDOMAIN,
    save_to_bq,
)


def main(
    ndays: int = typer.Option(
        28,
        help="Total days to import data from Zendesk.",
    ),
):
    export_date = datetime.now().strftime("%Y-%m-%d")
    zendesk_api = ZendeskAPI(
        {
            "subdomain": ZENDESK_SUBDOMAIN,
            "email": ZENDESK_API_EMAIL,
            "token": ZENDESK_API_KEY,
        }
    )
    macro_df = zendesk_api.create_macro_stat_df()

    save_to_bq(
        df=macro_df,
        table_name="zendesk_macro_usage",
        event_date=export_date,
        date_column="export_date",
    )

    ticket_df = zendesk_api.create_ticket_stat_df(
        created_at=(datetime.now() - timedelta(days=ndays)).strftime("%Y-%m-%d")
    )

    save_to_bq(
        df=ticket_df,
        table_name="zendesk_ticket",
        event_date=export_date,
        date_column="export_date",
    )


if __name__ == "__main__":
    typer.run(main)
