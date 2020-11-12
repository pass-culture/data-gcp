from unittest.mock import patch

from api.app import app


def test_root():
    # Given
    # When
    response = app.test_client().get("/")

    # Then
    assert response.status_code == 200
    assert b"Recommandation" in response.data


def test_check():
    # Given
    # When
    response = app.test_client().get("/check")

    # Then
    assert response.status_code == 200
    assert response.data == b"OK"


class GetRecommendations:
    @patch("api.app.API_TOKEN", "good_token")
    def test_get_recommendations_return_403_when_there_is_wrong_token(self):
        # Given
        api_token = "wrong_token"

        # When
        response = app.test_client().get(f"/get_recommendations?token={api_token}")

        # Then
        assert response.status_code == 403
        assert response.data == b"Forbidden"

    @patch("api.app.API_TOKEN", "good_token")
    def test_get_recommendations_return_200_when_there_is_right_token(self):
        # Given
        api_token = "good_token"

        # When
        response = app.test_client().get(f"/get_recommendations?token={api_token}")

        # Then
        assert response.status_code == 200
        assert response.data == b"Ok"
