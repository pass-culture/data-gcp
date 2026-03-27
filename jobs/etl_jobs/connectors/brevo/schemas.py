from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class ApiGlobalStats(BaseModel):
    """
    Global statistics for a campaign.
    Source: https://developers.brevo.com/reference/get-email-campaigns
    """

    delivered: int = 0
    sent: int = 0
    complaints: int = 0
    hard_bounces: int = Field(0, alias="hardBounces")
    soft_bounces: int = Field(0, alias="softBounces")
    clickers: int = 0
    unique_clicks: int = Field(0, alias="uniqueClicks")
    opens: int = Field(0, alias="opens")  # Sometimes called 'opens' or 'viewed'
    viewed: int = 0
    unique_views: int = Field(0, alias="uniqueViews")
    unsubscriptions: int = 0
    trackable_views: int = Field(0, alias="trackableViews")
    trackable_views_rate: float = Field(0.0, alias="trackableViewsRate")
    estimated_views: int = Field(0, alias="estimatedViews")

    model_config = ConfigDict(populate_by_name=True)


class ApiCampaignStatsItem(BaseModel):
    """
    Stats for a specific list within a campaign.
    """

    list_id: int = Field(..., alias="listId")
    delivered: int = 0
    sent: int = 0
    complaints: int = 0
    hard_bounces: int = Field(0, alias="hardBounces")
    soft_bounces: int = Field(0, alias="softBounces")
    clickers: int = 0
    unique_clicks: int = Field(0, alias="uniqueClicks")
    unique_views: int = Field(0, alias="uniqueViews")
    unsubscriptions: int = 0
    viewed: int = 0
    deferred: int = 0

    model_config = ConfigDict(populate_by_name=True)


class ApiCampaignStatistics(BaseModel):
    """Statistics wrapper for a campaign."""

    global_stats: ApiGlobalStats = Field(
        default_factory=ApiGlobalStats, alias="globalStats"
    )
    campaign_stats: List[ApiCampaignStatsItem] = Field(
        default_factory=list, alias="campaignStats"
    )
    mirror_click: int = Field(0, alias="mirrorClick")
    remaining: int = 0
    links_stats: Dict[str, Any] = Field(default_factory=dict, alias="linksStats")
    stats_by_domain: Dict[str, Any] = Field(default_factory=dict, alias="statsByDomain")
    stats_by_device: Dict[str, Any] = Field(default_factory=dict, alias="statsByDevice")
    stats_by_browser: Dict[str, Any] = Field(
        default_factory=dict, alias="statsByBrowser"
    )

    model_config = ConfigDict(populate_by_name=True)


class ApiSender(BaseModel):
    """Sender details."""

    id: Union[str, int, None] = None  # ID can sometimes be string or int
    name: Optional[str] = None
    email: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class ApiRecipients(BaseModel):
    """Recipients configuration."""

    lists: List[int] = Field(default_factory=list)
    exclusion_lists: List[int] = Field(default_factory=list, alias="exclusionLists")

    model_config = ConfigDict(populate_by_name=True)


class ApiCampaign(BaseModel):
    """
    Raw Campaign data from Brevo API.
    Mirrors 'Get email campaigns' response.
    """

    id: int
    name: str
    subject: Optional[str] = None
    type: Optional[str] = "classic"
    status: Optional[str] = None

    scheduled_at: Optional[str] = Field(None, alias="scheduledAt")
    created_at: Optional[str] = Field(None, alias="createdAt")
    modified_at: Optional[str] = Field(None, alias="modifiedAt")
    sent_date: Optional[str] = Field(
        None, alias="sentDate"
    )  # Deprecated or specific to some endpoints? 'scheduledAt' is common.

    tag: Optional[str] = None
    tags: List[str] = Field(
        default_factory=list
    )  # API doc shows 'tag' string, but some versions have list? Keeping permissive.

    share_link: Optional[str] = Field(None, alias="shareLink")
    test_sent: bool = Field(False, alias="testSent")
    header: Optional[str] = None
    footer: Optional[str] = None
    sender: Optional[ApiSender] = None
    reply_to: Optional[str] = Field(None, alias="replyTo")
    to_field: Optional[str] = Field(None, alias="toField")
    html_content: Optional[str] = Field(None, alias="htmlContent")
    inline_image_activation: bool = Field(False, alias="inlineImageActivation")
    mirror_active: bool = Field(False, alias="mirrorActive")
    recurring: bool = False
    preview_text: Optional[str] = Field(None, alias="previewText")

    recipients: Optional[ApiRecipients] = None
    statistics: ApiCampaignStatistics = Field(default_factory=ApiCampaignStatistics)

    model_config = ConfigDict(populate_by_name=True)


class ApiEvent(BaseModel):
    """Raw Event data from Brevo Transactional API (smtp/statistics/events)."""

    email: str

    event: str

    date: str  # API returns ISO string

    message_id: Optional[str] = Field(None, alias="messageId")

    subject: Optional[str] = None

    template_id: Optional[int] = Field(None, alias="templateId")

    tag: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class ApiTemplate(BaseModel):
    """


    SMTP Template details.


    Source: https://developers.brevo.com/reference/get-smtp-templates


    """

    id: int

    name: str

    subject: Optional[str] = None

    is_active: bool = Field(True, alias="isActive")

    test_sent: bool = Field(False, alias="testSent")

    tag: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class ApiTemplatesResponse(BaseModel):
    """Response from smtp/templates."""

    count: int = 0

    templates: List[ApiTemplate] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class ApiSmtpReportItem(BaseModel):
    """


    Aggregated report item for a specific date.


    Source: https://developers.brevo.com/reference/get-smtp-report


    """

    date: str

    requests: int = 0

    delivered: int = 0

    hard_bounces: int = Field(0, alias="hardBounces")

    soft_bounces: int = Field(0, alias="softBounces")

    clicks: int = 0

    unique_clicks: int = Field(0, alias="uniqueClicks")

    opens: int = 0

    unique_opens: int = Field(0, alias="uniqueOpens")

    spam_reports: int = Field(0, alias="spamReports")

    blocked: int = 0

    invalid: int = 0

    unsubscribed: int = 0

    model_config = ConfigDict(populate_by_name=True)


class ApiSmtpReport(BaseModel):
    """


    Response from smtp/statistics/reports


    """

    reports: List[ApiSmtpReportItem] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)
