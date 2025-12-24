import logging
from typing import Optional

from http_tools.clients import AsyncHttpClient, SyncHttpClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BrevoConnector:
    """
    Brevo API wrapper using SyncHttpClient for interacting with Brevo endpoints.
    """

    BASE_URL = "https://api.brevo.com/v3"

    def __init__(self, api_key: str, client: Optional[SyncHttpClient] = None):
        self.api_key = api_key
        self.client = client or SyncHttpClient()
        self.headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get_email_campaigns(
        self,
        status: str = "sent",
        limit: int = 50,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
    ):
        url = f"{self.BASE_URL}/emailCampaigns"
        params = {
            "status": status,
            "limit": limit,
            "offset": offset,
            "statistics": "globalStats",
        }
        if start_date:
            params["startDate"] = start_date

        if end_date:
            params["endDate"] = end_date

        return self.client.request("GET", url, headers=self.headers, params=params)

    def get_smtp_templates(self, active_only: bool = True, offset: int = 0):
        url = f"{self.BASE_URL}/smtp/templates"
        params = {"templateStatus": str(active_only).lower(), "offset": offset}
        return self.client.request("GET", url, headers=self.headers, params=params)

    def get_email_event_report(
        self,
        template_id: int,
        event: str,
        start_date: str,
        end_date: str,
        offset: int = 0,
    ):
        url = f"{self.BASE_URL}/smtp/statistics/events"
        params = {
            "templateId": template_id,
            "event": event,
            "startDate": start_date,
            "endDate": end_date,
            "offset": offset,
        }
        return self.client.request("GET", url, headers=self.headers, params=params)


class AsyncBrevoConnector:
    """
    Brevo API wrapper using AsyncHttpClient for non-blocking API interactions.
    """

    BASE_URL = "https://api.brevo.com/v3"

    def __init__(self, api_key: str, client: Optional[AsyncHttpClient] = None):
        self.api_key = api_key
        self.client = client or AsyncHttpClient()
        self.headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def get_email_campaigns(
        self, status: str = "sent", limit: int = 50, offset: int = 0
    ):
        url = f"{self.BASE_URL}/emailCampaigns"
        params = {"status": status, "limit": limit, "offset": offset}
        return await self.client.request(
            "GET", url, headers=self.headers, params=params
        )

    async def get_smtp_templates(self, active_only: bool = True, offset: int = 0):
        url = f"{self.BASE_URL}/smtp/templates"
        params = {"templateStatus": str(active_only).lower(), "offset": offset}
        return await self.client.request(
            "GET", url, headers=self.headers, params=params
        )

    async def get_email_event_report(
        self,
        template_id: int,
        event: str,
        start_date: str,
        end_date: str,
        offset: int = 0,
    ):
        url = f"{self.BASE_URL}/smtp/statistics/events"
        params = {
            "templateId": template_id,
            "event": event,
            "startDate": start_date,
            "endDate": end_date,
            "offset": offset,
        }
        return await self.client.request(
            "GET", url, headers=self.headers, params=params
        )
