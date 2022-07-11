from unittest.mock import patch, Mock

import pytest
from app import app


@pytest.mark.parametrize(
    "materialized_view_name",
    ["recommendable_offers", "non_recommendable_offers", "iris_venues_mv"],
)
@patch("app.get_materialized_view_status")
def test_health_checks(get_materialized_view_status_mock: Mock, materialized_view_name):
    # Given
    get_materialized_view_status_mock.return_value = {
        f"is_{materialized_view_name}_datasource_exists": False,
        f"is_{materialized_view_name}_ok": False,
    }
    # When
    response = app.test_client().get(f"/health/{materialized_view_name}")

    # Then
    assert response.status_code == 200
    assert response.json == {
        f"is_{materialized_view_name}_datasource_exists": False,
        f"is_{materialized_view_name}_ok": False,
    }


def test_home():
    # Given
    # When
    response = app.test_client().get("/")

    # Then
    assert response.status_code == 200


def test_check():
    # Given
    # When
    response = app.test_client().get("/check")

    # Then
    assert response.status_code == 200
    assert response.data == b"OK"


@patch("app.API_TOKEN", "good_token")
def test_recommendation_return_403_when_there_is_wrong_token():
    # Given
    api_token = "wrong_token"
    user_id = 1

    # When
    response = app.test_client().get(f"/recommendation/{user_id}?token={api_token}")

    # Then
    assert response.status_code == 403
    assert response.data == b"Forbidden"
