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
