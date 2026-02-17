import logging
from typing import Optional

from connectors.brevo.config import BASE_URL
from http_tools.clients import AsyncHttpClient, SyncHttpClient

# Logger here is for Business Logic / Progress tracking
logger = logging.getLogger(__name__)


class BrevoConnector:
    BASE_URL = BASE_URL

    def __init__(self, client: SyncHttpClient):
        self.client = client

    def get_email_campaigns(
        self,
        status: str = "sent",
        limit: int = 50,
        offset: int = 0,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
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

        # Business Context Log
        logger.info(f"📊 Fetching email campaigns (status={status}, offset={offset})")

        return self.client.request("GET", url, params=params)

    def get_smtp_templates(self, active_only: bool = True, offset: int = 0):
        url = f"{self.BASE_URL}/smtp/templates"
        params = {"templateStatus": str(active_only).lower(), "offset": offset}

        logger.info(f"📋 Listing SMTP templates (active_only={active_only})")

        return self.client.request("GET", url, params=params)

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

        # Business Context Log - very useful for long ETL paginations
        logger.info(
            f"🔍 Fetching events for Template {template_id} | Type: {event} | Offset: {offset}"
        )

        return self.client.request("GET", url, params=params)


class AsyncBrevoConnector:
    BASE_URL = BASE_URL

    def __init__(self, client: AsyncHttpClient):
        self.client = client

    async def get_email_campaigns(
        self, status: str = "sent", limit: int = 50, offset: int = 0
    ):
        url = f"{self.BASE_URL}/emailCampaigns"
        params = {"status": status, "limit": limit, "offset": offset}

        logger.info(f"📊 [Async] Fetching campaigns offset {offset}")
        return await self.client.request("GET", url, params=params)

    async def get_smtp_templates(self, active_only: bool = True, offset: int = 0):
        url = f"{self.BASE_URL}/smtp/templates"
        params = {"templateStatus": str(active_only).lower(), "offset": offset}

        logger.info("📋 [Async] Listing SMTP templates")
        return await self.client.request("GET", url, params=params)

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

        # This log helps you see progress in the middle of hundreds of concurrent tasks
        logger.info(f"🔍 [Async] Template {template_id} | Page Offset {offset}")

        return await self.client.request("GET", url, params=params)
