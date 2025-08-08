import logging
from datetime import datetime

import pandas as pd
import typer
from connectors.instagram import InstagramConnector

from .config import (
    INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
    INSTAGRAM_ACCOUNT_DAILY_ACTIVITY_DTYPE,
    INSTAGRAM_ACCOUNT_INSIGHTS,
    INSTAGRAM_ACCOUNT_INSIGHTS_DTYPE,
    INSTAGRAM_ACCOUNTS_ID,
    INSTAGRAM_API_RATE_LIMIT,
    INSTAGRAM_API_VERSION,
    INSTAGRAM_POST_DETAIL,
    INSTAGRAM_POST_DETAIL_DTYPE,
)
from .utils import (
    ACCESS_TOKEN,
    add_deprecated_metrics,
    df_to_bq,
    handle_metric_replacements,
    preprocess_insight_data,
    preprocess_insight_posts,
    save_multiple_partitions_to_bq,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_daily_stats(start_date: str, end_date: str) -> None:
    """
    Process daily statistics for all Instagram accounts.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    """
    logger.info(f"Processing daily stats from {start_date} to {end_date}")

    dfs = []

    for account_id in INSTAGRAM_ACCOUNTS_ID:
        logger.info(f"Processing daily stats for account {account_id}")

        try:
            # Initialize Instagram connector
            instagram_connector = InstagramConnector(
                account_id=account_id,
                access_token=ACCESS_TOKEN,
                api_version=INSTAGRAM_API_VERSION,
                use_async=False,  # Use sync for simplicity
                requests_per_minute=INSTAGRAM_API_RATE_LIMIT,
            )

            # Fetch daily insights
            logger.info(f"Fetching daily insights for account {account_id}")
            insights_data = instagram_connector.fetch_daily_insights_data(
                start_date, end_date
            )

            if not insights_data:
                logger.warning(f"No insights data retrieved for account {account_id}")
                continue

            # Process insights data
            account_daily_insights_df = preprocess_insight_data(insights_data)

            if account_daily_insights_df.empty:
                logger.warning(f"No processed insights for account {account_id}")
                continue

            # Add account ID
            account_daily_insights_df["account_id"] = account_id

            # Handle deprecated metrics and replacements for v23.0 compatibility
            account_daily_insights_df = handle_metric_replacements(
                account_daily_insights_df
            )
            account_daily_insights_df = add_deprecated_metrics(
                account_daily_insights_df, "profile"
            )

            dfs.append(account_daily_insights_df)
            logger.info(
                f"Successfully processed {len(account_daily_insights_df)} daily insight records for account {account_id}"
            )

        except Exception as e:
            logger.error(f"Failed to process daily stats for account {account_id}: {e}")
            raise

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Saving combined daily stats: {len(combined_df)} total records")

        save_multiple_partitions_to_bq(
            combined_df,
            INSTAGRAM_ACCOUNT_DAILY_ACTIVITY,
            start_date=start_date,
            end_date=end_date,
            date_column="event_date",
            schema=INSTAGRAM_ACCOUNT_DAILY_ACTIVITY_DTYPE,
        )
        logger.info("Successfully saved daily stats to BigQuery")
    else:
        logger.warning("No daily stats data to save")


def process_account_insights(export_date: str) -> None:
    """
    Process lifetime account insights for all Instagram accounts.

    Args:
        export_date: Export date in YYYY-MM-DD format
    """
    logger.info(f"Processing account insights for export date {export_date}")

    dfs = []

    for account_id in INSTAGRAM_ACCOUNTS_ID:
        logger.info(f"Processing account insights for account {account_id}")

        try:
            # Initialize Instagram connector
            instagram_connector = InstagramConnector(
                account_id=account_id,
                access_token=ACCESS_TOKEN,
                api_version=INSTAGRAM_API_VERSION,
                use_async=False,
                requests_per_minute=INSTAGRAM_API_RATE_LIMIT,
            )

            # Fetch lifetime account insights
            account_insights_json = (
                instagram_connector.fetch_lifetime_account_insights_data()
            )

            if account_insights_json is not None:
                account_insights_df = pd.DataFrame([account_insights_json])
                account_insights_df["export_date"] = export_date
                dfs.append(account_insights_df)
                logger.info(
                    f"Successfully processed account insights for account {account_id}"
                )
            else:
                logger.error(f"Could not retrieve account insights for {account_id}")
                raise Exception(f"Failed to get account insights for {account_id}")

        except Exception as e:
            logger.error(
                f"Failed to process account insights for account {account_id}: {e}"
            )
            raise

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Saving combined account insights: {len(combined_df)} records")

        df_to_bq(
            combined_df,
            INSTAGRAM_ACCOUNT_INSIGHTS,
            event_date=export_date,
            date_column="export_date",
            schema=INSTAGRAM_ACCOUNT_INSIGHTS_DTYPE,
        )
        logger.info("Successfully saved account insights to BigQuery")
    else:
        logger.warning("No account insights data to save")


def process_posts(export_date: str) -> None:
    """
    Process posts and their insights for all Instagram accounts.

    Args:
        export_date: Export date in YYYY-MM-DD format
    """
    logger.info(f"Processing posts for export date {export_date}")

    dfs = []

    for account_id in INSTAGRAM_ACCOUNTS_ID:
        logger.info(f"Processing posts for account {account_id}")

        try:
            # Initialize Instagram connector
            instagram_connector = InstagramConnector(
                account_id=account_id,
                access_token=ACCESS_TOKEN,
                api_version=INSTAGRAM_API_VERSION,
                use_async=False,
                requests_per_minute=INSTAGRAM_API_RATE_LIMIT,
            )

            # Fetch posts data
            logger.info(f"Fetching posts for account {account_id}")
            posts = instagram_connector.fetch_posts_data()

            if not posts:
                logger.warning(f"No posts retrieved for account {account_id}")
                continue

            # Process post insights
            logger.info(
                f"Processing insights for {len(posts)} posts from account {account_id}"
            )
            post_insights_df = preprocess_insight_posts(posts, instagram_connector)

            if post_insights_df.empty:
                logger.warning(f"No processed post insights for account {account_id}")
                continue

            # Add metadata
            post_insights_df["export_date"] = export_date
            post_insights_df["account_id"] = account_id

            # Handle deprecated metrics for v23.0 compatibility
            post_insights_df = handle_metric_replacements(post_insights_df)
            post_insights_df = add_deprecated_metrics(post_insights_df, "post")

            dfs.append(post_insights_df)
            logger.info(
                f"Successfully processed {len(post_insights_df)} post insight records for account {account_id}"
            )

        except Exception as e:
            logger.error(f"Failed to process posts for account {account_id}: {e}")
            raise

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Saving combined post insights: {len(combined_df)} records")

        df_to_bq(
            combined_df,
            INSTAGRAM_POST_DETAIL,
            event_date=export_date,
            date_column="export_date",
            schema=INSTAGRAM_POST_DETAIL_DTYPE,
        )
        logger.info("Successfully saved post insights to BigQuery")
    else:
        logger.warning("No post insights data to save")


def main(
    start_date: str = typer.Option(
        ...,
        help="Start date for exporting accounts data (YYYY-MM-DD format).",
    ),
    end_date: str = typer.Option(
        ...,
        help="End date for exporting accounts data (YYYY-MM-DD format).",
    ),
    jobs: str = typer.Option(
        "account,post,daily_stats",
        help="Jobs to process. Options: account, post, daily_stats",
    ),
):
    """
    Main entry point for Instagram data extraction job.

    This script processes Instagram data using the Graph API v23.0 with comprehensive
    error handling, rate limiting, and deprecated metrics compatibility.
    """
    logger.info("=" * 60)
    logger.info("Instagram Data Extraction Job Started")
    logger.info("=" * 60)
    logger.info(f"Start Date: {start_date}")
    logger.info(f"End Date: {end_date}")
    logger.info(f"Jobs: {jobs}")
    logger.info(f"API Version: {INSTAGRAM_API_VERSION}")
    logger.info(f"Accounts: {INSTAGRAM_ACCOUNTS_ID}")

    # Validate inputs
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        raise typer.Exit(1)

    if not ACCESS_TOKEN:
        logger.error("Facebook access token not available")
        raise typer.Exit(1)

    job_list = [job.strip().lower() for job in jobs.split(",")]
    export_date = datetime.now().strftime("%Y-%m-%d")

    valid_jobs = {"account", "post", "daily_stats"}
    invalid_jobs = set(job_list) - valid_jobs
    if invalid_jobs:
        logger.error(
            f"Invalid jobs specified: {invalid_jobs}. Valid jobs: {valid_jobs}"
        )
        raise typer.Exit(1)

    try:
        # Process daily stats
        if "daily_stats" in job_list:
            logger.info("Processing daily stats job")
            process_daily_stats(start_date, end_date)

        # Process account insights
        if "account" in job_list:
            logger.info("Processing account insights job")
            process_account_insights(export_date)

        # Process posts
        if "post" in job_list:
            logger.info("Processing posts job")
            process_posts(export_date)

        logger.info("=" * 60)
        logger.info("Instagram Data Extraction Job Completed Successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        logger.info("=" * 60)
        logger.info("Instagram Data Extraction Job Failed")
        logger.info("=" * 60)
        raise typer.Exit(1)


if __name__ == "__main__":
    typer.run(main)
