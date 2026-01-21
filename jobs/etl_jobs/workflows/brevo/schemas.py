from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class CleanCampaign(BaseModel):
    """Cleaned Campaign data for BigQuery loading."""

    campaign_id: int
    campaign_name: str
    campaign_utm: Optional[str] = None
    campaign_sent_date: Optional[str] = (
        None  # Keeping as str to match existing transform logic, can be datetime
    )
    share_link: Optional[str] = None
    audience_size: int
    open_number: int
    unsubscriptions: int
    update_date: datetime
    campaign_target: str

    model_config = ConfigDict(from_attributes=True)


class CleanTransactionalEvent(BaseModel):
    """Cleaned Transactional Event data (pivoted) for BigQuery loading."""

    template: int
    tag: Optional[str] = None
    email: str
    event_date: date
    delivered_count: int = 0
    opened_count: int = 0
    unsubscribed_count: int = 0
    target: str

    model_config = ConfigDict(from_attributes=True)
