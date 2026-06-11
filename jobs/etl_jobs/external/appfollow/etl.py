"""
AppFollow ETL Processing Module.

This module provides classes for extracting, transforming, and loading
AppFollow analytics data into BigQuery.
"""

from typing import Any, Dict, List

import pandas as pd
from jobs.etl_jobs.external.appfollow.client import AppFollowAPIError, AppFollowClient
from jobs.etl_jobs.external.appfollow.utils import (
    APPFOLLOW_REVIEWS,
    APPFOLLOW_REVIEWS_SCHEMA,
    save_to_bq,
)
from loguru import logger


class AppFollowETL:
    """
    AppFollow ETL processor for analytics data.

    This class handles the extraction, transformation, and loading
    of AppFollow reviews data into BigQuery.
    """

    def __init__(self, client: AppFollowClient):
        """
        Initialize the AppFollow ETL processor.

        Args:
            client: Authenticated AppFollowClient instance
        """
        self.client = client

    def extract_reviews_data(
        self, ext_id: str, from_date: str, to_date: str
    ) -> List[Dict[str, Any]]:
        """
        Extract all reviews for a specific app within a date range.

        Args:
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            list: List of raw reviews data
        """

        try:
            reviews = self.client.get_all_reviews(ext_id, from_date, to_date)
            return reviews
        except AppFollowAPIError as e:
            logger.error(f"Error extracting AppFollow reviews data for: {ext_id}", e)
            return []

    def transform_reviews_data(self, reviews: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw AppFollow reviews data into a DataFrame.

        Args:
        reviews: List of raw reviews data

        Returns:
        pd.DataFrame: Transformed reviews data
        """

        transformed_reviews = [
            {
                "review_id": str(review["review_id"]),
                "date": review["date"],
                "time": review.get("time"),
                "title": review.get("title", ""),
                "rating": review.get("rating"),
                "content": review.get("content", ""),
                "store": review.get("store"),
                "answer_text": review.get("answer_text"),
                "answer_date": review.get("answer_date"),
            }
            for review in reviews
        ]

        return pd.DataFrame(transformed_reviews)

    def load_reviews_data(
        self, reviews_df: pd.DataFrame, ext_id: str, from_date: str, to_date: str
    ) -> None:
        """
        Load transformed reviews data to BigQuery.

        Args:
            reviews_df: Transformed DataFrame
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
        """
        if reviews_df.empty:
            logger.warning("No reviews data available to load")
            return None

        logger.info(f"Loading {len(reviews_df)} records into BigQuery")

        reviews_df["date"] = pd.to_datetime(reviews_df["date"])
        reviews_df["ext_id"] = ext_id

        logger.info("Saving.. {} -> {}", APPFOLLOW_REVIEWS, from_date)
        save_to_bq(
            df=reviews_df,
            table_name=APPFOLLOW_REVIEWS,
            schema_field=APPFOLLOW_REVIEWS_SCHEMA,
            start_date=from_date,
            end_date=to_date,
            date_column="date",
        )
        logger.success(f"Successfully loaded {len(reviews_df)} records for {ext_id}")

    def run_etl(self, ext_id: str, from_date: str, to_date: str) -> bool:
        """
        Run the complete ETL process for AppFollow data.

        Args:
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            bool: True if ETL completed successfully, False otherwise
        """

        try:
            logger.info(f"Starting AppFollow ETL process for ext_id: {ext_id}")
            logger.info(f"Date range: {from_date} to {to_date}")

            raw_reviews = self.extract_reviews_data(ext_id, from_date, to_date)

            if not raw_reviews:
                logger.info("Failed to extract reviews data")
                return False

            transformed_df = self.transform_reviews_data(raw_reviews)

            self.load_reviews_data(transformed_df, ext_id, from_date, to_date)

            logger.success(f"AppFollow ETL completed successfully for ext_id: {ext_id}")
            return True
        except Exception as e:
            logger.error(f"AppFollow ETL process failed for {ext_id}: {e}")
            return False
