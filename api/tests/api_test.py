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
def test_recommendation_return_recommended_offeres_when_there_is_right_token(
    get_recommendations_for_user_mock,
):
    # Given
    api_token = "good_token"
    user_id = 1
    get_recommendations_for_user_mock.return_value = [1, 2, 3]

    # When
    response = app.test_client().get(f"/recommendation/{user_id}?token={api_token}")

    # Then
    assert response.status_code == 200
    assert response.data == b'{"recommended offers ":[1,2,3]}\n'
