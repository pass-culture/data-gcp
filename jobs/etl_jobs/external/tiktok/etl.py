"""
TikTok ETL Processing Module.

This module provides classes for extracting, transforming, and loading
TikTok analytics data into BigQuery.
"""

from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from client import TikTokAPIError, TikTokClient
from utils import (
    TIKTOK_ACCOUNT_DAILY_ACTIVITY,
    TIKTOK_ACCOUNT_HOURLY_AUDIENCE,
    TIKTOK_VIDEO_AUDIENCE_COUNTRY,
    TIKTOK_VIDEO_AUDIENCE_COUNTRY_SCHEMA,
    TIKTOK_VIDEO_DETAIL,
    TIKTOK_VIDEO_DETAIL_SCHEMA,
    TIKTOK_VIDEO_IMPRESSION_SOURCE,
    TIKTOK_VIDEO_IMPRESSION_SOURCE_SCHEMA,
    save_to_bq,
)


class TikTokETL:
    """
    TikTok ETL processor for analytics data.

    This class handles the extraction, transformation, and loading
    of TikTok analytics data into BigQuery.
    """

    def __init__(self, client: TikTokClient):
        """
        Initialize the TikTok ETL processor.

        Args:
            client: Authenticated TikTokClient instance
        """
        self.client = client

    def extract_account_data(
        self,
        account_id: str,
        start_date: str,
        end_date: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Extract account analytics data with logging.

        Args:
            account_id: Business or Creator account ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            dict: Account analytics data or None if failed
        """
        analytics_fields = [
            "username",
            "display_name",
            "profile_image",
            "audience_countries",
            "audience_genders",
            "likes",
            "comments",
            "shares",
            "followers_count",
            "profile_views",
            "video_views",
            "audience_activity",
        ]
        try:
            account_stats = self.client.get_account_analytics(
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
                fields=analytics_fields,
            )
            if "metrics" in account_stats.get("data", {}):
                logger.success("Successfully retrieved historical analytics")
                return account_stats
            logger.warning("Account data not available")
            return None
        except TikTokAPIError as e:
            logger.error("Error extracting account data: {}", e)
            return None

    def extract_videos_data(self, account_id: str) -> List[Dict[str, Any]]:
        """
        Extract all videos data for a business account.

        Args:
            account_id: Business or Creator account ID

        Returns:
            list: List of videos data
        """
        fields = [
            "item_id",
            "create_time",
            "thumbnail_url",
            "caption",
            "share_url",
            "embed_url",
            "video_views",
            "likes",
            "comments",
            "shares",
            "reach",
            "video_duration",
            "full_video_watched_rate",
            "total_time_watched",
            "average_time_watched",
            "impression_sources",
            "audience_countries",
        ]
        try:
            videos = self.client.get_all_videos(account_id, fields)
            return videos
        except TikTokAPIError as e:
            logger.error("Error extracting videos data: {}", e)
            return []

    def transform_hourly_activity_data(
        self, json_data: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Transform hourly activity data into a DataFrame.

        Args:
            json_data: Raw hourly activity data

        Returns:
            pd.DataFrame: Transformed hourly activity data
        """
        hourly_activity_data = []
        for entry in json_data:
            date = entry["date"]
            for activity in entry.get("audience_activity", []):
                hourly_activity_data.append(
                    {
                        "date": date,
                        "hour": activity["hour"],
                        "audience": activity["count"],
                    }
                )
        return pd.DataFrame(hourly_activity_data)

    def transform_daily_activity_data(
        self, json_data: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Transform daily activity data into a DataFrame.

        Args:
            json_data: Raw daily activity data

        Returns:
            pd.DataFrame: Transformed daily activity data
        """
        daily_activity_data = [
            {
                "date": entry["date"],
                "video_views": entry.get("video_views", 0),
                "likes": entry.get("likes", 0),
                "followers_count": entry.get("followers_count", 0),
                "shares": entry.get("shares", 0),
                "profile_views": entry.get("profile_views", 0),
                "comments": entry.get("comments", 0),
            }
            for entry in json_data
        ]
        return pd.DataFrame(daily_activity_data)

    def transform_impression_sources_data(
        self, videos: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Transform impression sources data into a DataFrame.

        Args:
            videos: List of video data

        Returns:
            pd.DataFrame: Transformed impression sources data
        """
        impression_sources_data = []
        for video in videos:
            item_id = video["item_id"]
            for source in video.get("impression_sources", []):
                impression_sources_data.append(
                    {
                        "item_id": item_id,
                        "impression_source": source["impression_source"],
                        "percentage": source["percentage"],
                    }
                )
        return pd.DataFrame(impression_sources_data)

    def transform_audience_countries_data(
        self, videos: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Transform audience countries data into a DataFrame.

        Args:
            videos: List of video data

        Returns:
            pd.DataFrame: Transformed audience countries data
        """
        audience_countries_data = []
        for video in videos:
            item_id = video["item_id"]
            for country in video.get("audience_countries", []):
                audience_countries_data.append(
                    {
                        "item_id": item_id,
                        "country": country["country"],
                        "percentage": country["percentage"],
                    }
                )
        return pd.DataFrame(audience_countries_data)

    def transform_videos_data(self, videos: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transform videos data into a DataFrame.

        Args:
            videos: List of video data

        Returns:
            pd.DataFrame: Transformed videos data
        """
        videos_data = [
            {
                "item_id": video["item_id"],
                "caption": video["caption"],
                "average_time_watched": video["average_time_watched"],
                "reach": video["reach"],
                "create_time": video["create_time"],
                "share_url": video["share_url"],
                "video_views": video["video_views"],
                "total_time_watched": video["total_time_watched"],
                "likes": video["likes"],
                "shares": video["shares"],
                "comments": video["comments"],
                "video_duration": video["video_duration"],
                "full_video_watched_rate": video["full_video_watched_rate"],
                "thumbnail_url": video["thumbnail_url"],
                "embed_url": video["embed_url"],
            }
            for video in videos
        ]
        return pd.DataFrame(videos_data)

    def load_account_data(
        self,
        username: str,
        account_data: Dict[str, Any],
        start_date: str,
        end_date: str,
    ) -> Optional[str]:
        """
        Load account analytics data to BigQuery.

        Args:
            account_data: Account analytics data
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            str: Account username or None if failed
        """
        if not account_data or "metrics" not in account_data.get("data", {}):
            logger.warning("No metrics data available to load")
            return None

        metrics_data = account_data["data"]["metrics"]

        logger.info("Loading account data for {}", username)
        # We have historical analytics data
        logger.info("Loading {} historical records", len(metrics_data))

        # Load hourly audience activity data
        hourly_activity_df = self.transform_hourly_activity_data(metrics_data)
        hourly_activity_df["date"] = pd.to_datetime(hourly_activity_df["date"])
        if not hourly_activity_df.empty:
            hourly_activity_df["account"] = str(username)
            logger.info("Saving.. {} -> {}", TIKTOK_ACCOUNT_HOURLY_AUDIENCE, start_date)
            save_to_bq(
                df=hourly_activity_df,
                table_name=TIKTOK_ACCOUNT_HOURLY_AUDIENCE,
                start_date=start_date,
                end_date=end_date,
                date_column="date",
            )
            logger.success("Loaded {} hourly activity records", len(hourly_activity_df))

        # Load daily activity data
        daily_activity_df = self.transform_daily_activity_data(metrics_data)
        daily_activity_df["date"] = pd.to_datetime(daily_activity_df["date"])
        daily_activity_df["account"] = str(username)
        logger.info("Saving.. {} -> {}", TIKTOK_ACCOUNT_DAILY_ACTIVITY, start_date)
        save_to_bq(
            df=daily_activity_df,
            table_name=TIKTOK_ACCOUNT_DAILY_ACTIVITY,
            start_date=start_date,
            end_date=end_date,
            date_column="date",
        )
        logger.success("Loaded {} daily activity records", len(daily_activity_df))

    def load_videos_data(
        self, videos_data: List[Dict[str, Any]], account_username: str, export_date: str
    ) -> None:
        """
        Load videos data to BigQuery.

        Args:
            videos_data: List of videos data
            account_username: Account username
            export_date: Export date in YYYY-MM-DD format
        """
        if not videos_data:
            logger.info("No videos data to load")
            return

        logger.info(f"Loading videos data for {account_username}")

        # Load impression sources data
        impression_sources_df = self.transform_impression_sources_data(videos_data)
        if not impression_sources_df.empty:
            impression_sources_df["account"] = str(account_username)
            impression_sources_df["export_date"] = export_date
            save_to_bq(
                df=impression_sources_df,
                table_name=TIKTOK_VIDEO_IMPRESSION_SOURCE,
                start_date=export_date,
                end_date=export_date,
                date_column="export_date",
                schema_field=TIKTOK_VIDEO_IMPRESSION_SOURCE_SCHEMA,
            )
            logger.info(
                f"Loaded {len(impression_sources_df)} impression sources records"
            )

        # Load audience countries data
        audience_countries_df = self.transform_audience_countries_data(videos_data)
        if not audience_countries_df.empty:
            audience_countries_df["account"] = str(account_username)
            audience_countries_df["export_date"] = export_date
            save_to_bq(
                df=audience_countries_df,
                table_name=TIKTOK_VIDEO_AUDIENCE_COUNTRY,
                start_date=export_date,
                end_date=export_date,
                date_column="export_date",
                schema_field=TIKTOK_VIDEO_AUDIENCE_COUNTRY_SCHEMA,
            )
            logger.info(
                f"Loaded {len(audience_countries_df)} audience countries records"
            )

        # Load videos detail data
        videos_df = self.transform_videos_data(videos_data)
        videos_df["account"] = str(account_username)
        videos_df["export_date"] = export_date
        save_to_bq(
            df=videos_df,
            table_name=TIKTOK_VIDEO_DETAIL,
            start_date=export_date,
            end_date=export_date,
            schema_field=TIKTOK_VIDEO_DETAIL_SCHEMA,
            date_column="export_date",
        )
        logger.info(f"Loaded {len(videos_df)} video detail records")

    def run_etl(self, account_id: str, start_date: str, end_date: str) -> bool:
        """
        Run the complete ETL process for TikTok data.

        Args:
            account_id: Business or Creator account ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            bool: True if ETL completed successfully, False otherwise
        """
        try:
            logger.info("Starting TikTok ETL for account_id: {}", account_id)
            logger.info("Date range: {} to {}", start_date, end_date)
            # Extract and load account data
            logger.info("Extracting account analytics data...")
            account_data = self.extract_account_data(account_id, start_date, end_date)

            if not account_data:
                logger.error("Failed to extract account data")
                return False
            # Extract username
            account_username = account_data["data"]["username"]

            if not account_username:
                logger.error("Failed to load account data")
                return False

            # Extract and load videos data
            logger.info("Extracting videos data...")
            videos_data = self.extract_videos_data(account_id)

            # Extract and load videos data
            logger.info("Loading videos data to Bigquery...")
            self.load_videos_data(videos_data, account_username, end_date)
            # Extract and load account data
            logger.info("Loading account data to Bigquery...")
            self.load_account_data(account_username, account_data, start_date, end_date)

            logger.success("TikTok ETL completed successfully")
            return True

        except Exception as e:
            logger.error("ETL process failed: {}", e)
            return False
