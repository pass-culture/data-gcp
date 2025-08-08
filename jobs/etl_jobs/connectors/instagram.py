import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import pandas as pd
from http_tools.clients import AsyncHttpClient, SyncHttpClient
from http_tools.rate_limiters import TokenBucketRateLimiter

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Maximum error rate allowed before failing
MAX_ERROR_RATE = 0.2


class InstagramConnector:
    """
    Instagram Graph API v23.0 connector with rate limiting and comprehensive error handling.
    Handles deprecated metrics and maintains compatibility with existing schema.
    """

    def __init__(
        self,
        account_id: str,
        access_token: str,
        api_version: str = "v23.0",
        use_async: bool = False,
        requests_per_minute: int = 200,  # Instagram API default limit
    ):
        """
        Initialize Instagram connector.

        Args:
            account_id: Instagram account ID
            access_token: Facebook/Meta access token
            api_version: Graph API version (default: v23.0)
            use_async: Whether to use async HTTP client
            requests_per_minute: API rate limit
        """
        self.account_id = account_id
        self.access_token = access_token
        self.api_version = api_version
        self.graph_uri = f"https://graph.facebook.com/{api_version}/"

        # Initialize rate limiter
        rate_limiter = TokenBucketRateLimiter(
            requests_per_minute=requests_per_minute,
            burst_capacity=requests_per_minute // 2,  # Allow some burst capacity
        )

        # Initialize HTTP client
        if use_async:
            self.client = AsyncHttpClient(
                rate_limiter=rate_limiter,
                timeout=30.0,
                max_retries=3,
                retry_status_codes=[500, 502, 503, 504],
            )
            self._is_async = True
        else:
            self.client = SyncHttpClient(
                rate_limiter=rate_limiter,
                timeout=30,
                max_retries=3,
                retry_status_codes=[500, 502, 503, 504],
            )
            self._is_async = False

        logger.info(
            f"InstagramConnector initialized for account {account_id} "
            f"using API {api_version} with {'async' if use_async else 'sync'} client"
        )

    def _make_request(
        self, method: str, endpoint: str, params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Make HTTP request with comprehensive error handling.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response JSON or None on error
        """
        url = f"{self.graph_uri}{endpoint}"

        # Add access token to params
        if params is None:
            params = {}
        params["access_token"] = self.access_token

        try:
            if self._is_async:
                # For async, we need to run in event loop
                if asyncio.get_event_loop().is_running():
                    response = asyncio.create_task(
                        self.client.request(method, url, params=params)
                    )
                else:
                    response = asyncio.run(
                        self.client.request(method, url, params=params)
                    )
            else:
                response = self.client.request(method, url, params=params)

            if response is None:
                logger.error(f"Request failed: {method} {url}")
                return None

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(
                    f"API error {response.status_code}: {response.text} "
                    f"for {method} {url} with params {params}"
                )
                return None

        except Exception as e:
            logger.error(f"Unexpected error in request {method} {url}: {e}")
            return None

    def fetch_daily_insights_data(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Fetch daily insights data with v23.0 API compatibility.
        Handles deprecated metrics and maps to new ones.

        Args:
            start_date: Start date in 'YYYY-MM-dd' format
            end_date: End date in 'YYYY-MM-dd' format

        Returns:
            List of daily insights data

        Raises:
            RuntimeError: If more than MAX_ERROR_RATE of requests fail
            ValueError: If start_date > end_date
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if start_dt > end_dt:
            raise ValueError("start_date must be earlier than or equal to end_date.")

        logger.info(f"Fetching daily insights from {start_date} to {end_date}")

        period = "day"
        data = []
        error_count = 0
        total_requests = 0

        # Updated metric types for v23.0 API
        # Note: 'impressions' deprecated, replaced with 'views'
        # Note: Some profile metrics deprecated but we keep them for schema compatibility
        METRIC_TYPES = {
            "total_value": ["views"],  # Replaces old 'views' with total_value type
            "default": [
                "reach",
                "follower_count",
            ],
        }

        total_days = (end_dt - start_dt).days + 1

        for day in range(total_days):
            day_date = start_dt + timedelta(days=day)
            since = int(day_date.timestamp())
            until = int((day_date + timedelta(days=1)).timestamp()) - 1

            logger.debug(f"Fetching insights for {day_date.strftime('%Y-%m-%d')}")

            # Make separate calls for each metric type
            for metric_type, metrics in METRIC_TYPES.items():
                if not metrics:
                    continue

                total_requests += 1
                params = {
                    "metric": ",".join(metrics),
                    "period": period,
                    "since": since,
                    "until": until,
                }

                # Add metric_type parameter if needed
                if metric_type != "default":
                    params["metric_type"] = metric_type

                logger.debug(f"Request params: {params}")

                response_data = self._make_request(
                    "GET", f"{self.account_id}/insights", params
                )

                if response_data:
                    data.append(response_data)
                    logger.debug(
                        f"Successfully fetched {metric_type} metrics for {day_date.strftime('%Y-%m-%d')}"
                    )
                else:
                    error_count += 1
                    logger.error(
                        f"Failed to fetch {metric_type} metrics for {day_date.strftime('%Y-%m-%d')}"
                    )

        # Check error rate
        error_rate = error_count / total_requests if total_requests > 0 else 0
        if error_rate > MAX_ERROR_RATE:
            raise RuntimeError(
                f"More than {MAX_ERROR_RATE*100}% of requests failed "
                f"({error_count}/{total_requests} errors)"
            )

        logger.info(
            f"Successfully fetched {len(data)} insight records with {error_count} errors"
        )
        return data

    def fetch_lifetime_account_insights_data(self) -> Optional[Dict]:
        """
        Fetch lifetime account insights using v23.0 API.

        Returns:
            Account insights data or None on error
        """
        logger.info(f"Fetching lifetime insights for account {self.account_id}")

        # Updated fields for v23.0 - using IG User endpoints
        metrics = [
            "followers_count",
            "follows_count",
            "media_count",
            "id",
            "biography",
            "name",
            "username",
        ]

        params = {
            "fields": ",".join(metrics),
        }

        response_data = self._make_request("GET", self.account_id, params)

        if response_data:
            logger.info("Successfully fetched lifetime account insights")
        else:
            logger.error("Failed to fetch lifetime account insights")

        return response_data

    def fetch_posts_data(self) -> List[Dict]:
        """
        Fetch all Instagram posts with pagination support.

        Returns:
            List of posts data
        """
        logger.info(f"Fetching posts for account {self.account_id}")

        posts_data = []

        # Initial request
        params = {
            "fields": "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp"
        }

        response_data = self._make_request("GET", f"{self.account_id}/media", params)

        if not response_data:
            logger.error("Failed to fetch initial posts data")
            return []

        posts_data.extend(response_data.get("data", []))
        next_url = response_data.get("paging", {}).get("next")

        # Handle pagination
        while next_url:
            logger.debug(f"Fetching next page: {len(posts_data)} posts so far")

            # Extract URL without base domain for our client
            if next_url.startswith("https://graph.facebook.com/"):
                endpoint = next_url.replace("https://graph.facebook.com/", "")
                # Remove access token from URL as we add it in _make_request
                if "access_token=" in endpoint:
                    endpoint = endpoint.split("&access_token=")[0]

                response_data = self._make_request("GET", endpoint)

                if response_data:
                    new_posts = response_data.get("data", [])
                    posts_data.extend(new_posts)
                    next_url = response_data.get("paging", {}).get("next")
                    logger.info(
                        f"Imported {len(new_posts)} posts (total: {len(posts_data)})"
                    )
                else:
                    logger.error("Failed to fetch paginated posts data")
                    break
            else:
                logger.error(f"Unexpected pagination URL format: {next_url}")
                break

        logger.info(f"Successfully fetched {len(posts_data)} total posts")
        return posts_data

    def fetch_post_insights(self, media_id: str, media_type: str) -> Optional[Dict]:
        """
        Fetch insights for a specific post using v23.0 API.
        Handles deprecated metrics and maps them appropriately.

        Args:
            media_id: The media ID of the post
            media_type: The media type of the post

        Returns:
            Post insights data or None on error
        """
        logger.debug(f"Fetching insights for post {media_id} (type: {media_type})")

        # Updated metrics for v23.0 API
        # Note: 'plays' deprecated for videos, 'video_views' partially deprecated
        # We use 'views' as replacement where applicable
        default_metrics = [
            "views",  # Replaces 'impressions' and 'plays' in many cases
            "shares",
            "comments",
            "likes",
            "saved",
            "total_interactions",
            "follows",
            "profile_visits",
            "profile_activity",
            "reach",
        ]

        # Media type specific metrics
        metric_dict = {
            "VIDEO": [
                "shares",
                "comments",
                "likes",
                "saved",
                "total_interactions",
                "reach",
                "views",  # Use 'views' instead of deprecated 'plays'
            ],
            "IMAGE": default_metrics,
            "CAROUSEL_ALBUM": default_metrics,
        }

        metrics = metric_dict.get(media_type, default_metrics)

        params = {"metric": ",".join(metrics)}

        response_data = self._make_request("GET", f"{media_id}/insights", params)

        if response_data:
            logger.debug(f"Successfully fetched insights for post {media_id}")
        else:
            logger.debug(f"Failed to fetch insights for post {media_id}")

        return response_data

    def get_stats(self) -> Dict:
        """Get connector statistics."""
        stats = self.client.get_stats()
        stats.update(
            {
                "account_id": self.account_id,
                "api_version": self.api_version,
                "client_type": "async" if self._is_async else "sync",
            }
        )
        return stats

    async def close(self):
        """Close async resources if using async client."""
        if self._is_async and hasattr(self.client, "close"):
            await self.client.close()
