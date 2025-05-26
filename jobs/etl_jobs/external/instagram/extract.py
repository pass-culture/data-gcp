from datetime import datetime, timedelta

import pandas as pd
import requests

MAX_ERROR_RATE = 0.1


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

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        if start_dt > end_dt:
            raise ValueError("start_date must be earlier than or equal to end_date.")

        period = "day"
        data = []
        error_count = 0
        total_requests = 0
        # Metric type mapping for Instagram Graph API
        METRIC_TYPES = {
            "total_value": ["views"],  # metrics that need total_value
            "default": [
                "reach",
                "follower_count",
            ],  # metrics that don't need special type
        }

        total_days = (end_dt - start_dt).days + 1

        for day in range(total_days):
            day_date = start_dt + timedelta(days=day)
            since = int(day_date.timestamp())
            until = int((day_date + timedelta(days=1)).timestamp()) - 1

            # Make separate calls for each metric type
            for metric_type, metrics in METRIC_TYPES.items():
                if not metrics:  # Skip if no metrics for this type
                    continue

                total_requests += 1
                params = {
                    "metric": ",".join(metrics),
                    "period": period,
                    "since": since,
                    "until": until,
                    "access_token": self.access_token,
                }

                # Add metric_type parameter if needed
                if metric_type != "default":
                    params["metric_type"] = metric_type

                response = requests.get(
                    f"{self.graph_uri}{self.account_id}/insights", params=params
                )

                if response.status_code == 200:
                    data.append(response.json())
                else:
                    error_count += 1
                    print(
                        f"Error fetching daily insights data for {metric_type} metrics: {response.status_code} - {response.text}"
                    )

        error_rate = error_count / total_requests
        if error_rate > MAX_ERROR_RATE:
            raise RuntimeError(
                f"More than {MAX_ERROR_RATE*100}% of requests failed ({error_count}/{total_requests} errors)"
            )

        return data

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
            data_points = insight.get("data", [])
            row = {}
            for data_point in data_points:
                metric_name = data_point.get("name")
                value = data_point.get("values", [{}])[0].get("value")
                end_time = data_point.get("values", [{}])[0].get("end_time")
                row[metric_name] = value
                row["event_date"] = end_time
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
        insights_data = self.fetch_daily_insights_data(start_date, end_date)
        df_insights = self.preprocess_insight_data(insights_data)
        df_insights["account_id"] = self.account_id
        for c in deprecated_metrics:
            df_insights[c] = 0
        return df_insights

    def fetch_lifetime_account_insights_data(self) -> dict:
        """
        Fetches lifetime insights.

        Returns:
            list: A list of lifetime insights data.
        """

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
            "access_token": self.access_token,
        }

        response = requests.get(f"{self.graph_uri}{self.account_id}", params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(
                f"Error fetching daily insights data: {response.status_code} - {response.text}"
            )
            return None

    def _get_instagram_posts(self) -> dict:
        """
        Fetches Instagram posts for the account.

        Returns:
            dict: JSON response containing posts data.
        """
        url = f"{self.graph_uri}{self.account_id}/media"
        params = {
            "fields": "id,caption,media_type,media_url,thumbnail_url,permalink,timestamp",
            "access_token": self.access_token,
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(
                f"Error fetching Instagram posts: {response.status_code} - {response.text}"
            )
            return {}

    def fetch_posts(self) -> list:
        """
        Fetches all Instagram posts, handling pagination.

        Returns:
            list: A list of posts data.
        """
        posts_response = self._get_instagram_posts()
        posts_data = posts_response.get("data", [])
        next_page = posts_response.get("paging", {}).get("next")

        while next_page:
            response = requests.get(next_page)
            if response.status_code == 200:
                posts_response = response.json()
                posts_data.extend(posts_response.get("data", []))
                next_page = posts_response.get("paging", {}).get("next")
                print(f"Importing {len(posts_response.get('data', []))} posts...")
            else:
                print(f"Error fetching posts: {response.status_code} - {response.text}")
                break

        return posts_data

    def _get_post_insights(self, media_id: str, media_type: str) -> dict:
        """
        Fetches insights for a specific post.

        Args:
            media_id (str): The media ID of the post.
            media_type (str): The media type of the post.

        Returns:
            dict: JSON response containing insights data.
        """
        default_metrics = [
            "impressions",
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
        metric_dict = {
            "VIDEO": [
                "shares",
                "comments",
                "likes",
                "saved",
                "total_interactions",
                "reach",
            ],
            "IMAGE": default_metrics,
            "CAROUSEL_ALBUM": default_metrics,
        }

        metrics = metric_dict.get(media_type, default_metrics)
        url = f"{self.graph_uri}{media_id}/insights"
        params = {"metric": ",".join(metrics), "access_token": self.access_token}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(
                f"Error fetching post insights: {response.status_code} - {response.text}"
            )
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
        error_count = 0
        total_posts = len(posts)

        for post in posts:
            post_insights = self._get_post_insights(
                post.get("id"), post.get("media_type")
            )

            if post_insights is None:
                error_count += 1
                continue

            row = {
                "post_id": post.get("id"),
                "media_type": post.get("media_type"),
                "caption": post.get("caption"),
                "media_url": post.get("media_url"),
                "permalink": post.get("permalink"),
                "posted_at": post.get("timestamp"),
                "url_id": post.get("permalink", "").rstrip("/").split("/")[-1],
            }

            for data_point in post_insights.get("data", []):
                metric_name = data_point.get("name")
                value = data_point.get("values", [{}])[0].get("value")
                row[metric_name] = value

            rows.append(row)

        error_rate = error_count / total_posts
        if error_rate > MAX_ERROR_RATE:
            raise RuntimeError(
                f"More than {MAX_ERROR_RATE*100}% of post insight requests failed ({error_count}/{total_posts} errors)"
            )

        return pd.DataFrame(rows)

    def fetch_and_preprocess_posts(self, export_date: datetime) -> pd.DataFrame:
        """
        Fetches and preprocesses Instagram posts into a DataFrame with additional metadata.

        Returns:
            pd.DataFrame: Processed posts data with added 'export_date' and 'account_id' columns.
        """
        deprecated_metrics = [
            "video_views",
        ]
        posts = self.fetch_posts()
        df_posts = self.preprocess_insight_posts(posts)
        df_posts["export_date"] = export_date
        df_posts["account_id"] = self.account_id
        for c in deprecated_metrics:
            df_posts[c] = 0
        return df_posts
