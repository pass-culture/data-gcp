from datetime import datetime

import typer

from extract import InstagramAnalytics
from utils import (
    ACCESS_TOKEN,
    ACCOUNT_ID,
    INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
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
):
    instagram_handler = InstagramAnalytics(
        account_id=ACCOUNT_ID, access_token=ACCESS_TOKEN
    )
    print(f"Fetching account {ACCOUNT_ID} insights from {start_date} to {end_date}")
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
    print(f"Fetching posts from {ACCOUNT_ID} insights")
    export_date = datetime.now().strftime("%Y-%m-%d")
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
