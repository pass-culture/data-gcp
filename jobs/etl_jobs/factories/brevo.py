from connectors.brevo.auth import BrevoAuthManager
from connectors.brevo.client import AsyncBrevoConnector, BrevoConnector
from connectors.brevo.limiter import AsyncBrevoRateLimiter, SyncBrevoRateLimiter
from http_tools.clients import AsyncHttpClient, SyncHttpClient
from utils.gcp import access_secret_data

from jobs.brevo.config import GCP_PROJECT, get_api_configuration


class BrevoFactory:
    @classmethod
    def create_connector(
        cls, audience: str, is_async: bool = False, max_concurrent: int = 5
    ):
        # 1. Get configuration strings from config.py
        secret_name, _ = get_api_configuration(audience)

        # 2. Fetch the actual key from Secret Manager
        api_key = access_secret_data(GCP_PROJECT, secret_name)

        # 3. Build Auth Manager
        auth = BrevoAuthManager(api_key)

        if is_async:
            limiter = AsyncBrevoRateLimiter(max_concurrent=max_concurrent)
            client = AsyncHttpClient(rate_limiter=limiter, auth_manager=auth)
            return AsyncBrevoConnector(client=client)

        limiter = SyncBrevoRateLimiter()
        client = SyncHttpClient(rate_limiter=limiter, auth_manager=auth)
        return BrevoConnector(client=client)
