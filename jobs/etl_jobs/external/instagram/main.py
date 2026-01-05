from datetime import datetime

import pandas as pd
import typer
from loguru import logger

from extract import InstagramAnalytics
from utils import (
    ACCESS_TOKEN,
    INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
    INSTAGRAM_ACCOUNT_DAILY_ACTIVITY_DTYPE,
    INSTAGRAM_ACCOUNT_INSIGHTS,
    INSTAGRAM_ACCOUNT_INSIGHTS_DTYPE,
    INSTAGRAM_ACCOUNTS_ID,
    INSTAGRAM_POST_DETAIL,
    INSTAGRAM_POST_DETAIL_DTYPE,
    df_to_bq,
    save_multiple_partitions_to_bq,
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
):
    job_list = jobs.split(",")
    export_date = datetime.now().strftime("%Y-%m-%d")

    if "daily_stats" in job_list:
        dfs = []
        for account_id in INSTAGRAM_ACCOUNTS_ID:
            instagram_handler = InstagramAnalytics(
                account_id=account_id, access_token=ACCESS_TOKEN
            )
            logger.info(
                f"Fetching account {account_id} daily insights from {start_date} to {end_date}"
            )

            account_daily_insights_df = instagram_handler.fetch_and_preprocess_insights(
                start_date=start_date, end_date=end_date
            )
            dfs.append(account_daily_insights_df)
        save_multiple_partitions_to_bq(
            pd.concat(dfs, ignore_index=True),
            INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
            start_date=start_date,
            end_date=end_date,
            date_column="event_date",
            schema=INSTAGRAM_ACCOUNT_DAILY_ACTIVITY_DTYPE,
        )
    if "account" in job_list:
        dfs = []
        for account_id in INSTAGRAM_ACCOUNTS_ID:
            instagram_handler = InstagramAnalytics(
                account_id=account_id, access_token=ACCESS_TOKEN
            )
            logger.info(f"Fetching account {account_id} insights")

            account_insights_json = (
                instagram_handler.fetch_lifetime_account_insights_data()
            )
            if account_insights_json is not None:
                account_insights_df = pd.DataFrame([account_insights_json])
                account_insights_df["export_date"] = export_date
                dfs.append(account_insights_df)

            else:
                logger.error("Could not determine account stats")
                raise Exception()
        df_to_bq(
            pd.concat(dfs, ignore_index=True),
            INSTAGRAM_ACCOUNT_INSIGHTS,
            event_date=export_date,
            date_column="export_date",
            schema=INSTAGRAM_ACCOUNT_INSIGHTS_DTYPE,
        )
    if "post" in job_list:
        dfs = []
        for account_id in INSTAGRAM_ACCOUNTS_ID:
            instagram_handler = InstagramAnalytics(
                account_id=account_id, access_token=ACCESS_TOKEN
            )
            logger.info(f"Fetching posts from {account_id} insights")

            post_insights_df = instagram_handler.fetch_and_preprocess_posts(
                export_date="extract_date"
            )
            dfs.append(post_insights_df)
        df_to_bq(
            pd.concat(dfs, ignore_index=True),
            INSTAGRAM_POST_DETAIL,
            event_date=export_date,
            date_column="export_date",
            schema=INSTAGRAM_POST_DETAIL_DTYPE,
        )


if __name__ == "__main__":
    typer.run(main)
