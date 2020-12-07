from unittest.mock import patch

from app import app


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


@patch("app.API_TOKEN", "good_token")
@patch("app.get_recommendations_for_user")
@patch("app.get_scored_recommendation_for_user")
@patch("app.get_iris_from_coordinates")
def test_recommendation_return_recommended_offers_when_there_is_right_token(
    get_iris_from_coordinates_mock,
    get_scored_recommendation_for_user_mock,
    get_recommendations_for_user_mock,
):
    # Given
    api_token = "good_token"
    user_id = 1
    get_recommendations_for_user_mock.return_value = [
        {"id": 1, "url": "url1", "type": "type1"},
        {"id": 2, "url": "url2", "type": "type2"},
        {"id": 3, "url": "url3", "type": "type3"},
    ]
    get_scored_recommendation_for_user_mock.return_value = [
        {"id": 1, "url": "url1", "type": "type1", "score": 1},
        {"id": 2, "url": "url2", "type": "type2", "score": 2},
        {"id": 3, "url": "url3", "type": "type3", "score": 3},
    ]
    get_iris_from_coordinates_mock.return_value = 1

    # When
    response = app.test_client().get(
        f"/recommendation/{user_id}?token={api_token}&latitude=2.331289&longitude=48.830719"
    )

    # Then
    assert response.status_code == 200
    assert response.data == b'{"recommended_offers":[3,2,1],"user_iris_id":1}\n'
