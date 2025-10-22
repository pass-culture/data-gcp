"""Fixtures and test data for the titelive test suite."""

from unittest.mock import Mock, patch

import pytest


@pytest.fixture(scope="session", autouse=True)
def mock_gcp_secret_manager():
    """
    Global mock for GCP Secret Manager to avoid authentication issues during tests.
    This fixture automatically applies to all tests and mocks the access_secret_data
    function before any module imports happen.
    """
    with patch("src.utils.gcp.access_secret_data") as mock_access_secret:
        # Configure the mock to return appropriate test values
        def mock_secret_data(project_id, secret_id, version_id="latest"):
            if secret_id == "titelive_epagine_api_username":
                return "test_username"
            elif secret_id == "titelive_epagine_api_password":
                return "test_password"
            else:
                return f"mock_secret_value_{secret_id}"

        mock_access_secret.side_effect = mock_secret_data
        yield mock_access_secret


@pytest.fixture()
def sample_api_response():
    """Sample API response with nested article structure."""
    return {
        "result": [
            {
                "id": 123,
                "article": {
                    "1": {
                        "gencod": "9781234567890",
                        "datemodification": "15/10/2024",
                        "prix": 19.99,
                        "titre": "Test Book 1",
                    },
                    "2": {
                        "gencod": "9781234567891",
                        "datemodification": "16/10/2024",
                        "prix": 24.99,
                        "titre": "Test Book 1 - Different Edition",
                    },
                },
            },
            {
                "id": 456,
                "article": {
                    "1": {
                        "gencod": "9789876543210",
                        "datemodification": "17/10/2024",
                        "prix": 15.50,
                        "titre": "Test Book 2",
                    }
                },
            },
        ]
    }


@pytest.fixture()
def sample_api_response_list_format():
    """Sample API response with article as list instead of dict."""
    return {
        "result": [
            {
                "id": 789,
                "article": [
                    {
                        "gencod": "9781111111111",
                        "datemodification": "18/10/2024",
                        "prix": 12.99,
                        "titre": "Test Book 3",
                    }
                ],
            }
        ]
    }


@pytest.fixture()
def sample_api_response_dict_result():
    """Sample API response where result is a dict instead of list."""
    return {
        "result": {
            "0": {
                "id": 999,
                "article": {
                    "1": {
                        "gencod": "9782222222222",
                        "datemodification": "19/10/2024",
                        "prix": 29.99,
                        "titre": "Test Book 4",
                    }
                },
            }
        }
    }


@pytest.fixture()
def mock_bigquery_client():
    """Mock BigQuery client."""
    mock_client = Mock()
    mock_client.query.return_value.result.return_value = iter([])
    return mock_client


@pytest.fixture()
def mock_storage_client():
    """Mock Google Cloud Storage client."""
    mock_client = Mock()
    mock_bucket = Mock()
    mock_blob = Mock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    return mock_client


@pytest.fixture()
def mock_requests_session():
    """Mock requests.Session."""
    mock_session = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "image/jpeg"}
    mock_response.content = b"fake_image_data"
    mock_response.raise_for_status.return_value = None
    mock_session.get.return_value = mock_response
    return mock_session


@pytest.fixture()
def mock_token_manager():
    """Mock TokenManager for API client tests."""
    mock_manager = Mock()
    mock_manager.get_token.return_value = "test_token"
    mock_manager.refresh_token.return_value = "refreshed_token"
    return mock_manager


@pytest.fixture()
def mock_token_manager_with_secrets():
    """Mock TokenManager with credentials set."""
    with patch("src.api.auth.access_secret_data") as mock_access_secret:
        mock_access_secret.side_effect = lambda project_id, secret_id: {
            "titelive_epagine_api_username": "test_username",
            "titelive_epagine_api_password": "test_password",
        }.get(secret_id)

        from src.api.auth import TokenManager

        return TokenManager(project_id="test-project")


@pytest.fixture()
def mock_titelive_client(mock_token_manager):
    """Mock TiteliveClient with mocked dependencies."""
    from src.api.client import TiteliveClient

    client = TiteliveClient(token_manager=mock_token_manager)

    # Mock the http_client
    client.http_client = Mock()
    client.http_client.request = Mock()

    return client


@pytest.fixture()
def mock_api_response():
    """Mock Titelive API response."""
    return {
        "result": [
            {
                "id": 123,
                "article": {
                    "1": {
                        "gencod": "9781234567890",
                        "datemodification": "15/10/2024",
                        "prix": 19.99,
                        "titre": "Test Book",
                    }
                },
            }
        ]
    }
