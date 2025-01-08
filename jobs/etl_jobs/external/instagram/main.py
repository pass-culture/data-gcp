from datetime import datetime

import pandas as pd
import typer

from extract import InstagramAnalytics
from utils import (
    ACCESS_TOKEN,
    ACCOUNT_ID,
    INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
    INSTAGRAM_ACCOUNT_INSIGHTS,
    INSTAGRAM_POST_DETAIL,
    __save_to_bq,
    save_to_bq,
)


def main(
    start_date: str = typer.Option(
        ...,
        help="Start date for exporting accounts data.",
    ),
    end_date: str = typer.Option(
        ...,
        help="End date for exporting accounts data.",
    ),
    jobs: str = typer.Option(
        "account,post,daily_stats",
        help="Jobs to process. Options: account, post, daily_stats",
    ),
    account_id: str = typer.Option(
        ACCOUNT_ID,
        help="Instagram account ID to fetch data from.",
    ),
):
    job_list = jobs.split(",")
    export_date = datetime.now().strftime("%Y-%m-%d")
    instagram_handler = InstagramAnalytics(
        account_id=account_id, access_token=ACCESS_TOKEN
    )
    if "daily_stats" in job_list:
        print(
            f"Fetching account {account_id} daily insights from {start_date} to {end_date}"
        )

        account_insights_df = instagram_handler.fetch_and_preprocess_insights(
            start_date=start_date, end_date=end_date
        )
        save_to_bq(
            account_insights_df,
            INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
            start_date=start_date,
            end_date=end_date,
            date_column="event_date",
        )
    if "account" in job_list:
        print(f"Fetching account {account_id} insights")

        account_insights_json = instagram_handler.fetch_lifetime_account_insights_data()
        account_insights_df = pd.DataFrame([account_insights_json])
        account_insights_df["export_date"] = export_date

        save_to_bq(
            account_insights_df,
            INSTAGRAM_ACCOUNT_INSIGHTS,
            event_date=export_date,
            date_column="export_date",
        )
    if "post" in job_list:
        print(f"Fetching posts from {account_id} insights")

        post_insights_df = instagram_handler.fetch_and_preprocess_posts(
            export_date="extract_date"
        )
        __save_to_bq(
            post_insights_df,
            INSTAGRAM_POST_DETAIL,
            event_date=export_date,
            date_column="export_date",
        )


if __name__ == "__main__":
    typer.run(main)
