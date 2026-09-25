"""
AppFollow ETL Processing Module.

This module provides classes for extracting, transforming, and loading
AppFollow analytics data into BigQuery.
"""

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from loguru import logger

from client import AppFollowClient
from utils import (
    APPFOLLOW_RATINGS,
    APPFOLLOW_RATINGS_SCHEMA,
    APPFOLLOW_REVIEWS,
    APPFOLLOW_REVIEWS_SCHEMA,
    infer_store,
    replace_app_rows_in_bq,
)

DATASETS = ("reviews", "ratings")


class AppFollowETL:
    """
    AppFollow ETL processor for analytics data.

    This class handles the extraction, transformation, and loading
    of AppFollow reviews and ratings data into BigQuery.
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
    ) -> list[dict[str, Any]]:
        """
        Extract all reviews for a specific app within a date range.

        API errors are propagated: an empty list means the API returned no review,
        and the window will be emptied accordingly.

        Args:
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            list: List of raw reviews data
        """
        return self.client.get_all_reviews(ext_id, from_date, to_date)

    def transform_reviews_data(self, reviews: list[dict[str, Any]]) -> pd.DataFrame:
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
                "app_version": review.get("app_version"),
            }
            for review in reviews
        ]

        return pd.DataFrame(transformed_reviews)

    def load_reviews_data(
        self, reviews_df: pd.DataFrame, ext_id: str, from_date: str, to_date: str
    ) -> None:
        """
        Replace the app's reviews over the date window in BigQuery.

        Args:
            reviews_df: Transformed DataFrame
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
        """
        if not reviews_df.empty:
            reviews_df["date"] = pd.to_datetime(reviews_df["date"]).dt.normalize()
            reviews_df["ext_id"] = ext_id

        logger.info(f"Loading {len(reviews_df)} reviews into {APPFOLLOW_REVIEWS}")
        replace_app_rows_in_bq(
            df=reviews_df,
            table_name=APPFOLLOW_REVIEWS,
            schema_field=APPFOLLOW_REVIEWS_SCHEMA,
            start_date=from_date,
            end_date=to_date,
            ext_id=ext_id,
            date_column="date",
        )
        logger.success(f"Successfully loaded {len(reviews_df)} reviews for {ext_id}")

    def extract_ratings_data(
        self, ext_id: str, store: str, from_date: str, to_date: str
    ) -> list[dict[str, Any]]:
        """
        Extract daily cumulative worldwide ratings for a specific app.

        Args:
            ext_id: App external ID
            store: Store code ("as" or "gp")
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            list: List of raw ratings data (one item per day)
        """
        return self.client.get_all_ratings_history(
            ext_id=ext_id,
            store=store,
            from_date=from_date,
            to_date=to_date,
            countries=["all"],
        )

    def transform_ratings_data(
        self, ratings: list[dict[str, Any]], ext_id: str, store: str
    ) -> pd.DataFrame:
        """
        Transform raw AppFollow ratings history into a DataFrame.

        Values are kept as returned by the API (cumulative totals); ratings received
        per period are left to downstream SQL.

        Args:
            ratings: List of raw ratings data
            ext_id: App external ID
            store: Store code ("as" or "gp")

        Returns:
            pd.DataFrame: Transformed ratings data
        """
        imported_at = datetime.now(timezone.utc)
        transformed_ratings = [
            {
                "date": rating["date"],
                "ext_id": ext_id,
                "store": store,
                "country": "all",
                "rating_avg": rating.get("avg_rating"),
                "ratings_total": rating.get("stars"),
                "stars_1_total": rating.get("stars1"),
                "stars_2_total": rating.get("stars2"),
                "stars_3_total": rating.get("stars3"),
                "stars_4_total": rating.get("stars4"),
                "stars_5_total": rating.get("stars5"),
                "imported_at": imported_at,
            }
            for rating in ratings
        ]

        return pd.DataFrame(
            transformed_ratings, columns=list(APPFOLLOW_RATINGS_SCHEMA.keys())
        )

    def load_ratings_data(
        self, ratings_df: pd.DataFrame, ext_id: str, from_date: str, to_date: str
    ) -> None:
        """
        Replace the app's ratings over the date window in BigQuery.

        Args:
            ratings_df: Transformed DataFrame
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
        """
        logger.info(f"Loading {len(ratings_df)} ratings into {APPFOLLOW_RATINGS}")
        replace_app_rows_in_bq(
            df=ratings_df,
            table_name=APPFOLLOW_RATINGS,
            schema_field=APPFOLLOW_RATINGS_SCHEMA,
            start_date=from_date,
            end_date=to_date,
            ext_id=ext_id,
            date_column="date",
        )
        logger.success(f"Successfully loaded {len(ratings_df)} ratings for {ext_id}")

    def run_reviews_etl(self, ext_id: str, from_date: str, to_date: str) -> None:
        raw_reviews = self.extract_reviews_data(ext_id, from_date, to_date)
        logger.info(f"Extracted {len(raw_reviews)} reviews for {ext_id}")
        transformed_df = self.transform_reviews_data(raw_reviews)
        self.load_reviews_data(transformed_df, ext_id, from_date, to_date)

    def run_ratings_etl(self, ext_id: str, from_date: str, to_date: str) -> None:
        store = infer_store(ext_id)
        raw_ratings = self.extract_ratings_data(ext_id, store, from_date, to_date)
        logger.info(f"Extracted {len(raw_ratings)} daily ratings for {ext_id}")
        if not raw_ratings:
            # Keep existing rows rather than wiping the window on an empty history.
            logger.warning(f"No ratings history returned for {ext_id}, skipping load")
            return
        transformed_df = self.transform_ratings_data(raw_ratings, ext_id, store)
        self.load_ratings_data(transformed_df, ext_id, from_date, to_date)

    def run_etl(
        self,
        ext_id: str,
        from_date: str,
        to_date: str,
        datasets: tuple[str, ...] = DATASETS,
    ) -> bool:
        """
        Run the complete ETL process for AppFollow data.

        Args:
            ext_id: App external ID
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            datasets: Datasets to import among "reviews" and "ratings"

        Returns:
            bool: True if every dataset was imported successfully, False otherwise
        """
        logger.info(f"Starting AppFollow ETL process for ext_id: {ext_id}")
        logger.info(f"Date range: {from_date} to {to_date}, datasets: {datasets}")

        runners = {"reviews": self.run_reviews_etl, "ratings": self.run_ratings_etl}
        success = True
        for dataset in datasets:
            try:
                runners[dataset](ext_id, from_date, to_date)
            except Exception as e:
                logger.error(f"AppFollow {dataset} ETL failed for {ext_id}: {e}")
                success = False

        if success:
            logger.success(f"AppFollow ETL completed successfully for ext_id: {ext_id}")
        return success
