from datetime import date, datetime

import pytest
from connectors.brevo.schemas import (
    ApiCampaign,
    ApiCampaignStatistics,
    ApiEvent,
    ApiGlobalStats,
)
from pydantic import ValidationError
from workflows.brevo.schemas import TableCampaign, TableTransactionalEvent
from workflows.brevo.transform import (
    transform_campaigns_to_dataframe,
    transform_events_to_dataframe,
)

# ==========================================
# 1. READ CONTRACT TESTS (Connectors)
# ==========================================


def test_api_campaign_validation_valid():
    """Test that valid API response data is correctly parsed into ApiCampaign."""
    data = {
        "id": 123,
        "name": "Test Campaign",
        "tag": "test_tag",
        "sentDate": "2023-01-01T10:00:00.000Z",
        "shareLink": "http://example.com",
        "statistics": {
            "globalStats": {"delivered": 100, "unsubscriptions": 5, "uniqueViews": 50}
        },
    }
    campaign = ApiCampaign(**data)
    assert campaign.id == 123
    assert campaign.statistics.global_stats.unique_views == 50


def test_api_campaign_validation_invalid_missing_id():
    """Test that missing required fields in API response raise ValidationError."""
    data = {"name": "Test Campaign"}
    with pytest.raises(ValidationError):
        ApiCampaign(**data)


def test_api_event_validation():
    """Test parsing of raw API event data."""
    data = {
        "email": "test@example.com",
        "event": "delivered",
        "date": "2023-01-01T12:00:00.000Z",
        "messageId": "msg123",
    }
    event = ApiEvent(**data)
    assert event.email == "test@example.com"
    assert event.event == "delivered"


# ==========================================
# 2. WRITE CONTRACT TESTS (Workflows)
# ==========================================


def test_table_campaign_validation():
    """Test TableCampaign (Write Contract) constraints."""
    data = {
        "campaign_id": 123,
        "campaign_name": "Clean Campaign",
        "campaign_sent_date": "2023-01-01",
        "audience_size": 100,
        "open_number": 50,
        "unsubscriptions": 2,
        "update_date": datetime.now(),
        "campaign_target": "native",
    }
    clean = TableCampaign(**data)
    assert clean.campaign_id == 123


def test_table_transactional_event_validation():
    """Test TableTransactionalEvent (Write Contract) constraints."""
    data = {
        "template": 1,
        "email": "user@example.com",
        "event_date": date(2023, 1, 1),
        "delivered_count": 1,
        "target": "pro",
    }
    clean = TableTransactionalEvent(**data)
    assert clean.template == 1
    assert clean.delivered_count == 1


# ==========================================
# 3. TRANSFORM LAYER TESTS
# ==========================================


def test_transform_campaigns_to_dataframe():
    """Test mapping logic from ApiCampaign (Read) to TableCampaign (Write)."""
    campaigns = [
        ApiCampaign(
            id=1,
            name="C1",
            tag="tag1",
            sent_date="2023-01-01",
            share_link="link1",
            statistics=ApiCampaignStatistics(
                global_stats=ApiGlobalStats(
                    delivered=10, unsubscriptions=1, unique_views=5
                )
            ),
        )
    ]

    df = transform_campaigns_to_dataframe(campaigns, audience="test_audience")

    assert len(df) == 1
    assert df.iloc[0]["campaign_id"] == 1
    assert df.iloc[0]["audience_size"] == 10
    assert df.iloc[0]["campaign_target"] == "test_audience"
    # Verify columns match TableCampaign fields
    assert list(df.columns) == list(TableCampaign.model_fields.keys())


def test_transform_events_to_dataframe():
    """Test pivoting logic for transactional events."""
    events = [
        {
            "template": 1,
            "tag": "tag1",
            "email": "a@a.com",
            "event": "delivered",
            "event_date": date(2023, 1, 1),
        },
        {
            "template": 1,
            "tag": "tag1",
            "email": "a@a.com",
            "event": "opened",
            "event_date": date(2023, 1, 1),
        },
    ]

    df = transform_events_to_dataframe(events, audience="native")

    assert len(df) == 1
    assert df.iloc[0]["delivered_count"] == 1
    assert df.iloc[0]["opened_count"] == 1
    assert df.iloc[0]["target"] == "native"
    assert list(df.columns) == list(TableTransactionalEvent.model_fields.keys())
