"""
Harvestr ETL Processing Module.

This module provides classes for extracting, transforming, and loading
Harvestr analytics data into BigQuery.
"""

import json
from typing import Any

import pandas as pd
from harvestr.client import HarvestrAPIError, HarvestrClient
from harvestr.utils import (
    HARVESTR_MESSAGES,
    HARVESTR_MESSAGES_SCHEMA,
    save_to_bq,
)
from loguru import logger


class HarvestrETL:
    """
    Harvestr ETL processor for analytics data.

    This class handles the extraction, transformation, and loading
    of Harvestr messages data into BigQuery.
    """

    def __init__(self, client: HarvestrClient):
        """
        Initialize the Harvestr ETL processor.

        Args:
            client: Authenticated HarvestrClient instance
        """
        self.client = client

    def extract_messages_data(
        self, from_date: str, to_date: str
    ) -> list[dict[str, Any]]:
        """
        Extract all raw feedback messages within a date range.

        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            list: List of raw message data
        """

        try:
            messages = self.client.get_all_messages(from_date, to_date)
            return messages
        except HarvestrAPIError as e:
            logger.error(f"Error extracting Harvestr messages: {e}")
            return []

    def transform_messages_data(self, messages: list[dict[str, Any]]) -> pd.DataFrame:
        """
        Transform raw Harvestr messages into a DataFrame.

        Args:
            messages: List of raw message data from the Harvestr API

        Returns:
            pd.DataFrame: Transformed messages data
        """

        transformed_messages = [
            {
                "id": str(message["id"]),
                "client_id": message.get("clientId"),
                "created_at": message.get("createdAt"),
                "updated_at": message.get("updatedAt"),
                "integration_url": message.get("integrationUrl"),
                "integration_id": message.get("integrationId"),
                "title": message.get("title", ""),
                "content": message.get("content", ""),
                "channel": message.get("channel"),
                "archived": message.get("archived"),
                "bin": message.get("bin"),
                "requester_id": message.get("requesterId"),
                "submitter_id": message.get("submitterId"),
                "labels": json.dumps(message.get("labels") or []),
            }
            for message in messages
        ]

        df = pd.DataFrame(transformed_messages)

        for date_column in ("created_at", "updated_at"):
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce", utc=True)

        return df

    def load_messages_data(
        self, messages_df: pd.DataFrame, from_date: str, to_date: str
    ) -> None:
        """
        Load transformed messages into BigQuery.

        Args:
            messages_df: Transformed DataFrame of messages
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
        """
        if messages_df.empty:
            logger.warning("No messages data available to load")
            return None

        logger.info(f"Loading {len(messages_df)} records into BigQuery")

        save_to_bq(
            df=messages_df,
            table_name=HARVESTR_MESSAGES,
            start_date=from_date,
            end_date=to_date,
            schema_field=HARVESTR_MESSAGES_SCHEMA,
            date_column="created_at",
        )
        logger.success(f"Successfully loaded {len(messages_df)} messages")

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Run the complete ETL process for Harvestr data.

        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format

        Returns:
            bool: True if ETL completed successfully, False otherwise
        """

        try:
            logger.info(
                f"Starting Harvestr ETL process for date range: {from_date} to {to_date}"
            )

            raw_messages = self.extract_messages_data(from_date, to_date)

            if not raw_messages:
                logger.info("No messages extracted for the given date range")
                return False

            logger.info(f"Extracted {len(raw_messages)} messages from Harvestr")

            messages_df = self.transform_messages_data(raw_messages)

            logger.info(
                f"Transformed {len(messages_df)} messages "
                f"({len(messages_df.columns)} columns)"
            )

            self.load_messages_data(messages_df, from_date, to_date)

            logger.success(
                f"Harvestr ETL completed successfully for {from_date} to {to_date}"
            )
            return True
        except Exception as e:
            logger.error(f"Harvestr ETL process failed: {e}")
            return False
