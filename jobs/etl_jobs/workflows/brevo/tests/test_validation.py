import pytest
from connectors.brevo.schemas import ApiCampaign, ApiCampaignStatistics, ApiGlobalStats
from pydantic import ValidationError
from workflows.brevo.transform import transform_campaigns_to_dataframe


def test_api_campaign_validation_valid():
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
    data = {"name": "Test Campaign"}
    with pytest.raises(ValidationError):
        ApiCampaign(**data)


def test_transform_campaigns_to_dataframe():
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
