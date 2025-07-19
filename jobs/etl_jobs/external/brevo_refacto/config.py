import os
from dataclasses import dataclass
from typing import Optional

from google.cloud import secretmanager

BASE_URL = "https://api.brevo.com/v3"

@dataclass
class Config:
    """
    Configuration dataclass - modern Python way to handle configs.
    Fields without defaults must come first!
    """
    # Required fields (no defaults)
    environment: str
    gcp_project: str
    api_key: str
    raw_dataset: str
    tmp_dataset: str
    
    # Optional fields (with defaults)
    api_base_url: str = BASE_URL
    max_concurrent_requests: int = 2
    requests_per_hour: int = 280
    
    @classmethod
    def from_environment(cls, audience: str = "native") -> "Config":
        """
        Factory method pattern - creates Config from environment.
        This keeps your main code clean from environment logic.
        """
        env = os.environ.get("ENV_SHORT_NAME", "dev")
        project = os.environ.get("GCP_PROJECT")
        
        # Get API key based on audience
        secret_name = f"sendinblue-api-key-{env}"
        if audience == "pro":
            secret_name = f"sendinblue-pro-api-key-{env}"
            
        api_key = cls._get_secret(project, secret_name)
        
        return cls(
            environment=env,
            gcp_project=project,
            api_key=api_key,
            raw_dataset=f"raw_{env}",
            tmp_dataset=f"tmp_{env}"
        )
    
    @staticmethod
    def _get_secret(project_id: str, secret_id: str) -> str:
        """Retrieve secret from GCP Secret Manager"""
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            # For local development, try environment variable
            return os.environ.get(secret_id.upper().replace("-", "_"), "")

SCHEMAS = {
    "campaigns_histo_schema": {
        "campaign_id": "INTEGER",
        "campaign_utm": "STRING",
        "campaign_name": "STRING",
        "campaign_target": "STRING",
        "campaign_sent_date": "STRING",
        "share_link": "STRING",
        "update_date": "DATETIME",
        "audience_size": "INTEGER",
        "open_number": "INTEGER",
        "unsubscriptions": "INTEGER",
    },
    "transactional_histo_schema": {
        "template": "INTEGER",
        "tag": "STRING",
        "email": "STRING",
        "event_date": "DATE",
        "target": "STRING",
        "delivered_count": "INTEGER",
        "opened_count": "INTEGER",
        "unsubscribed_count": "INTEGER",
    },
}