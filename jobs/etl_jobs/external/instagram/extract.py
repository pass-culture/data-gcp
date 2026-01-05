from datetime import datetime, timedelta

import pandas as pd
import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MAX_ERROR_RATE = 0.2
TIMEOUT = 30  # seconds
RETRIES = 3  # Total number of retries
BACKOFF_FACTOR = 2
STATUS_FORCELIST = [429, 500, 502, 503, 504]  # Retry on these HTTP status codes


def get_requests_session() -> requests.Session:
    """
    Create a requests session with retry logic for transient failures.

    Retries on:
    - Connection errors
    - Timeouts
    - 5xx server errors
    - 429 rate limit errors
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        allowed_methods=["GET"],  # Only retry GET requests
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


class InstagramAnalytics:
    """
    A class to fetch and process Instagram account insights and posts data using the Instagram Graph API.
    """

    def __init__(self, account_id: str, access_token: str):
        """
        Initializes the InstagramAnalytics object.

        Args:
            account_id (str): The Instagram account ID.
            access_token (str): The access token for the Instagram Graph API.
        """
        self.account_id = account_id
        self.access_token = access_token
        self.graph_uri = "https://graph.facebook.com/v14.0/"
        self.session = get_requests_session()

    # ------------------------------------------------------------------
    # DAILY INSIGHTS
    # ------------------------------------------------------------------

    def _validate_date_range(
        self, start_date: str, end_date: str
    ) -> tuple[datetime, datetime]:
        """
        Validates that start_date is before end_date and converts to datetime objects.

        Raises:
            ValueError: If start_date is after end_date.
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if start_dt > end_dt:
            raise ValueError("start_date must be earlier than or equal to end_date.")
        return start_dt, end_dt

    def _metric_dispatch(self) -> dict:
        """
        Returns a dispatch table to replace conditional metric-type logic.

        Returns:
            dict: Keys are metric types, values are lambdas returning extra params.
        """
        return {
            "default": lambda _: {},  # default metrics require no extra parameter
            "total_value": lambda _: {
                "metric_type": "total_value"
            },  # metrics like 'views' require metric_type
        }

    def _build_insight_params(
        self,
        metrics: list[str],
        since: int,
        until: int,
        extra: dict,
    ) -> dict:
        """
        Builds request parameters for Instagram insights API call.

        Args:
            metrics (list[str]): List of metric names to fetch.
            since (int): Unix timestamp for start of day.
            until (int): Unix timestamp for end of day.
            extra (dict): Extra parameters based on metric type.

        Returns:
            dict: Full parameters for the GET request.
        """
        params = {
            "metric": ",".join(metrics),
            "period": "day",
            "since": since,
            "until": until,
            "access_token": self.access_token,
        }
        params.update(extra)  # Merge any extra params like metric_type
        return params

    def _execute_insight_request(
        self,
        params: dict,
        metric_type: str,
        day_date: datetime,
    ) -> dict | None:
        """
        Executes the GET request for Instagram insights and handles errors.

        Args:
            params (dict): Request parameters.
            metric_type (str): The type of metric being requested.
            day_date (datetime): Date for logging purposes.

        Returns:
            dict | None: JSON response if successful, None on failure.
        """
        try:
            response = self.session.get(
                f"{self.graph_uri}{self.account_id}/insights",
                params=params,
                timeout=TIMEOUT,
            )

            if response.status_code == 200:
                return response.json()

            # Log errors for non-200 responses
            logger.error(
                f"Error fetching daily insights for {metric_type}: "
                f"{response.status_code} - {response.text}"
            )
            return None

        except requests.exceptions.Timeout:
            logger.error(
                f"Timeout fetching daily insights for {metric_type} "
                f"on {day_date.strftime('%Y-%m-%d')} after retries"
            )
            return None

        except requests.exceptions.RequestException as exc:
            logger.error(
                f"Request exception fetching daily insights for {metric_type}: {exc}"
            )
            return None

    def fetch_daily_insights_data(self, start_date: str, end_date: str) -> list:
        """
        Fetches daily insights data between the specified start and end dates.

        Args:
            start_date (str): Start date in 'YYYY-MM-dd' format.
            end_date (str): End date in 'YYYY-MM-dd' format.

        Returns:
            list: A list of daily insights data.

        Raises:
            RuntimeError: If more than MAX_ERROR_RATE of requests fail.
        """
        start_dt, end_dt = self._validate_date_range(start_date, end_date)

        # Metric type mapping for Instagram Graph API
        metric_types = {
            "total_value": ["views"],  # metrics that need total_value
            "default": [
                "reach",
                "follower_count",
            ],  # metrics that don't need special type
        }

        dispatch = self._metric_dispatch()  # Dispatch table for metric types
        total_days = (end_dt - start_dt).days + 1

        results: list = []
        error_count = 0
        total_requests = 0

        # Loop through each day
        for offset in range(total_days):
            day_date = start_dt + timedelta(days=offset)
            since = int(day_date.timestamp())
            until = int((day_date + timedelta(days=1)).timestamp()) - 1
            logger.info(
                f"Fetching daily insights for {day_date.strftime('%Y-%m-%d')} "
                f"(day {offset + 1}/{total_days})"
            )

            # Loop through metric types
            for metric_type, metrics in metric_types.items():
                if not metrics:  # Skip if no metrics for this type
                    continue

                total_requests += 1

                # Build request params using dispatch table
                extra_params = dispatch.get(metric_type, lambda _: {})(metrics)
                params = self._build_insight_params(metrics, since, until, extra_params)

                # Execute the request and handle response
                payload = self._execute_insight_request(params, metric_type, day_date)
                if payload is None:
                    error_count += 1
                else:
                    results.append(payload)

        # Check error rate
        error_rate = error_count / total_requests
        if error_rate > MAX_ERROR_RATE:
            raise RuntimeError(
                f"More than {MAX_ERROR_RATE*100}% of requests failed "
                f"({error_count}/{total_requests} errors)"
            )

        return results

    # ------------------------------------------------------------------
    # INSIGHTS PREPROCESSING
    # ------------------------------------------------------------------

    def preprocess_insight_data(self, insights_data: list) -> pd.DataFrame:
        """
        Processes raw insights data into a pandas DataFrame.

        Args:
            insights_data (list): Raw insights data as returned by fetch_daily_insights_data.

        Returns:
            pd.DataFrame: Processed insights data.
        """
        rows = []

        for insight in insights_data:
            row = {}
            for dp in insight.get("data", []):
                # Extract metric name and value
                row[dp.get("name")] = dp.get("values", [{}])[0].get("value")
                # Record the end_time as event_date
                row["event_date"] = dp.get("values", [{}])[0].get("end_time")

            if row:
                rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
        return df

    def fetch_and_preprocess_insights(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetches and preprocesses Instagram insights data into a DataFrame with additional metadata.

        Args:
            start_date (str): Start date in 'YYYY-MM-dd' format.
            end_date (str): End date in 'YYYY-MM-dd' format.

        Returns:
            pd.DataFrame: Processed insights data with added 'account_id' column.
        """
        # Deprecated metrics that are no longer available in the API
        deprecated_metrics = [
            "email_contacts",
            "phone_call_clicks",
            "text_message_clicks",
            "get_directions_clicks",
            "website_clicks",
            "profile_views",
        ]

        insights = self.fetch_daily_insights_data(start_date, end_date)
        df = self.preprocess_insight_data(insights)
        df["account_id"] = self.account_id

        # Add columns for deprecated metrics, filled with zeros
        for col in deprecated_metrics:
            df[col] = 0

        return df

    # ------------------------------------------------------------------
    # LIFETIME INSIGHTS
    # ------------------------------------------------------------------

    def fetch_lifetime_account_insights_data(self) -> dict | None:
        """
        Fetches lifetime insights.

        Returns:
            dict | None: JSON response containing lifetime account insights, or None on error.
        """
        fields = [
            "followers_count",
            "follows_count",
            "media_count",
            "id",
            "biography",
            "name",
            "username",
        ]

        response = self.session.get(
            f"{self.graph_uri}{self.account_id}",
            params={"fields": ",".join(fields), "access_token": self.access_token},
            timeout=TIMEOUT,
        )

        if response.status_code == 200:
            return response.json()

        logger.error(
            f"Error fetching lifetime insights: {response.status_code} - {response.text}"
        )
        return None

    # ------------------------------------------------------------------
    # POSTS
    # ------------------------------------------------------------------

    def _get_instagram_posts(self) -> dict:
        """
        Fetches Instagram posts for the account.

        Returns:
            dict: JSON response containing posts data.
        """
        response = self.session.get(
            f"{self.graph_uri}{self.account_id}/media",
            params={
                "fields": "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp",
                "access_token": self.access_token,
            },
            timeout=TIMEOUT,
        )

        if response.status_code == 200:
            return response.json()

        logger.error(
            f"Error fetching Instagram posts: {response.status_code} - {response.text}"
        )
        return {}

    def fetch_posts(self) -> list:
        """
        Fetches all Instagram posts, handling pagination.

        Returns:
            list: A list of posts data.
        """
        posts = []
        response = self._get_instagram_posts()

        posts.extend(response.get("data", []))
        next_page = response.get("paging", {}).get("next")

        while next_page:
            resp = self.session.get(next_page, timeout=TIMEOUT)
            if resp.status_code != 200:
                logger.error(f"Error fetching posts: {resp.status_code} - {resp.text}")
                break

            payload = resp.json()
            posts.extend(payload.get("data", []))
            next_page = payload.get("paging", {}).get("next")

        return posts

    # ------------------------------------------------------------------
    # POST INSIGHTS
    # ------------------------------------------------------------------

    def _post_metric_dispatch(self) -> dict:
        """
        Dispatch table for post metric types to avoid if/else.

        Returns:
            dict: Keys are media types, values are lists of metrics to fetch.
        """
        default = [
            "views",
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

        return {
            "VIDEO": [
                "shares",
                "comments",
                "likes",
                "saved",
                "total_interactions",
                "reach",
            ],
            "IMAGE": default,
            "CAROUSEL_ALBUM": default,
        }

    def _get_post_insights(self, media_id: str, media_type: str) -> dict | None:
        """
        Fetches insights for a specific post.

        Args:
            media_id (str): The media ID of the post.
            media_type (str): The media type of the post.

        Returns:
            dict | None: JSON response containing insights data, or None on error.
        """
        metrics = self._post_metric_dispatch().get(
            media_type,
            self._post_metric_dispatch()["IMAGE"],
        )

        try:
            response = self.session.get(
                f"{self.graph_uri}{media_id}/insights",
                params={"metric": ",".join(metrics), "access_token": self.access_token},
                timeout=TIMEOUT,
            )

            if response.status_code == 200:
                return response.json()

            logger.error(
                f"Error fetching post insights {media_id}: "
                f"{response.status_code} - {response.text}"
            )
            return None

        except requests.exceptions.RequestException as exc:
            logger.error(f"Post insight request failed {media_id}: {exc}")
            return None

    def preprocess_insight_posts(self, posts: list) -> pd.DataFrame:
        """
        Processes posts data and their insights into a pandas DataFrame.

        Args:
            posts (list): List of posts data.

        Returns:
            pd.DataFrame: Processed posts insights data.

        Raises:
            RuntimeError: If more than MAX_ERROR_RATE of post insight requests fail.
        """
        rows = []
        errors = 0
        total = len(posts)

        for idx, post in enumerate(posts, 1):
            if idx % 10 == 0:
                logger.info(f"Processing post {idx}/{total}")

            insights = self._get_post_insights(post["id"], post["media_type"])
            if insights is None:
                errors += 1
                continue

            row = {
                "post_id": post["id"],
                "media_type": post["media_type"],
                "caption": post.get("caption"),
                "media_url": post.get("media_url"),
                "permalink": post.get("permalink"),
                "posted_at": post.get("timestamp"),
                "url_id": post.get("permalink", "").rstrip("/").split("/")[-1],
            }

            # Flatten metrics into row
            for dp in insights.get("data", []):
                row[dp["name"]] = dp.get("values", [{}])[0].get("value")

            rows.append(row)

        if total and (errors / total) > MAX_ERROR_RATE:
            raise RuntimeError(
                f"More than {MAX_ERROR_RATE*100}% of post insight requests failed "
                f"({errors}/{total})"
            )

        return pd.DataFrame(rows)

    def fetch_and_preprocess_posts(self, export_date: datetime) -> pd.DataFrame:
        """
        Fetches and preprocesses Instagram posts into a DataFrame with additional metadata.

        Args:
            export_date (datetime): Date of export for metadata column.

        Returns:
            pd.DataFrame: Processed posts data with added 'export_date' and 'account_id' columns.
        """
        deprecated_metrics = ["video_views"]

        posts = self.fetch_posts()
        df = self.preprocess_insight_posts(posts)
        df["export_date"] = export_date
        df["account_id"] = self.account_id

        # Add deprecated metric columns
        for col in deprecated_metrics:
            df[col] = 0

        return df
