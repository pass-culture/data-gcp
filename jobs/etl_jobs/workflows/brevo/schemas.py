from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class TableCampaign(BaseModel):
    """
    Cleaned Campaign data for BigQuery loading.
    Matches the schema expected by the 'newsletters' table.
    """

    campaign_id: int
    campaign_name: str
    campaign_utm: Optional[str] = None
    campaign_sent_date: Optional[str] = None
    share_link: Optional[str] = None
    audience_size: int = 0
    open_number: int = 0
    unsubscriptions: int = 0
    update_date: datetime
    campaign_target: str

    model_config = ConfigDict(from_attributes=True)


class TableTransactionalEvent(BaseModel):
    """
    Cleaned Transactional Event data (pivoted) for BigQuery loading.
    Matches the schema expected by the 'transactional' table.
    """

    template: int
    tag: Optional[str] = None
    email: str
    event_date: date
    delivered_count: int = 0
    opened_count: int = 0
    unsubscribed_count: int = 0
    target: str

    model_config = ConfigDict(from_attributes=True)
