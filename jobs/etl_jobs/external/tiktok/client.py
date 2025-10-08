"""
TikTok API Client for Business Analytics.

This module provides a client class for interacting with TikTok's Business and Creator APIs
to fetch account analytics and video data.
"""

import json
from typing import Any, Dict, List, Optional

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class TikTokAPIError(Exception):
    """Custom exception for TikTok API errors."""

    pass


class TikTokClient:
    """
    TikTok Business API client for fetching analytics and video data.

    This class handles authentication, API requests, and error handling
    for TikTok's Business API endpoints.
    """

    BASE_API_URL = "https://open.tiktokapis.com/v2/"
    BUSINESS_API_URL = "https://business-api.tiktok.com/open_api/v1.3/"
    ACCOUNT_ANALYTICS_ENDPOINT = "business/get/"
    ACCOUNT_VIDEOS_ENDPOINT = "business/video/list/"
    CREATOR_VIDEOS_ENDPOINT = "user/video/list/"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        """
        Initialize the TikTok client.

        Args:
            client_id: TikTok application client ID
            client_secret: TikTok application client secret
            refresh_token: TikTok refresh token for authentication
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token: Optional[str] = None
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        return session

    def authenticate(self) -> bool:
        """
        Authenticate with TikTok API using refresh token.

        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            response = self._refresh_access_token()
            if response.get("access_token"):
                self.access_token = response["access_token"]
                return True
            return False
        except Exception as e:
            logger.info(f"Authentication failed: {e}")
            return False

    def _refresh_access_token(self) -> Dict[str, Any]:
        """Refresh the access token using the refresh token."""
        url = f"{self.BASE_API_URL}oauth/token/"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        data = {
            "client_key": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }

        response = self.session.post(url, headers=headers, data=data)

        if response.status_code == 200:
            return response.json()
        else:
            raise TikTokAPIError(
                f"Token refresh failed: {response.status_code} - {response.text}"
            )

    def get_account_info(self) -> Dict[str, Any]:
        """
        Get account information using the access token.

        Returns:
            dict: Account information response
        """
        if not self.access_token:
            raise TikTokAPIError("Not authenticated. Call authenticate() first.")

        fields = [
            "open_id",
            "username",
            "display_name",
            "profile_image",
            "follower_count",
            "following_count",
            "likes_count",
            "video_count",
        ]

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self.access_token}",
        }

        url = f"{self.BASE_API_URL}user/info/?fields={','.join(fields)}"
        response = self.session.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise TikTokAPIError(
                f"Account info fetch failed: {response.status_code} - {response.text}"
            )

    def get_account_analytics(
        self,
        account_id: str,
        start_date: str,
        end_date: str,
        fields: List[str],
    ) -> Dict[str, Any]:
        """
        Get account analytics data using TikTok Business or Creator API.

        Args:
            account_id: Business or Creator account ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            fields: List of fields to retrieve
            api_type: 'business' or 'creator'

        Returns:
            dict: API response containing account analytics data
        """
        if not self.access_token:
            raise TikTokAPIError("Not authenticated. Call authenticate() first.")

        headers = {
            "Access-Token": self.access_token,
        }
        params = {
            "business_id": account_id,
            "fields": json.dumps(fields),
        }
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        url = f"{self.BUSINESS_API_URL}{self.ACCOUNT_ANALYTICS_ENDPOINT}"
        response = self.session.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            raise TikTokAPIError(
                f"Account analytics fetch failed: {response.status_code} - {response.text}"
            )

    def get_account_videos(
        self,
        account_id: str,
        fields: List[str],
        cursor: Optional[str] = None,
        max_count: int = 20,
    ) -> Dict[str, Any]:
        """
        Get account videos data using TikTok Business or Creator API.

        Args:
            account_id: Business or Creator account ID
            fields: List of fields to retrieve
            cursor: Pagination cursor
            max_count: Maximum number of videos to retrieve
            api_type: 'business' or 'creator'

        Returns:
            dict: API response containing videos data
        """
        if not self.access_token:
            raise TikTokAPIError("Not authenticated. Call authenticate() first.")

        headers = {
            "Access-Token": self.access_token,
        }
        params = {
            "business_id": account_id,
            "fields": json.dumps(fields),
            "max_count": max_count,
        }
        if cursor:
            params["cursor"] = cursor
        url = f"{self.BUSINESS_API_URL}{self.ACCOUNT_VIDEOS_ENDPOINT}"
        response = self.session.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            raise TikTokAPIError(
                f"Videos fetch failed: {response.status_code} - {response.text}"
            )

    def get_all_videos(
        self, account_id: str, fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Get all videos for a business or creator account with pagination.

        Args:
            account_id: Business or Creator account ID
            fields: List of fields to retrieve
            api_type: 'business' or 'creator'

        Returns:
            list: List of all videos data
        """
        videos_data = []
        has_more = True
        cursor = None
        page_count = 0

        while has_more:
            try:
                results = self.get_account_videos(
                    account_id=account_id,
                    fields=fields,
                    cursor=cursor,
                    max_count=20,
                )
                videos = results["data"].get("videos", [])
                cursor = results["data"].get("cursor")
                has_more = results["data"].get("has_more", False)

                videos_data.extend(videos)
                page_count += 1
                logger.debug(f"Imported page {page_count} with {len(videos)} videos")

            except TikTokAPIError as e:
                logger.error(f"Error fetching videos page {page_count + 1}: {e}")
                has_more = False

        return videos_data
