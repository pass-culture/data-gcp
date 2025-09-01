"""Fixtures and test data for the test suite."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest


@pytest.fixture()
def sample_responses():
    """Load sample API responses from test data file."""
    data_file = Path(__file__).parent / "data" / "sample_responses.json"
    with open(data_file) as f:
        return json.load(f)


@pytest.fixture()
def mock_titelive_api_response(sample_responses):
    """Mock response from Titelive API search endpoint."""
    return sample_responses["sample_titelive_response"]


@pytest.fixture()
def mock_music_api_response(sample_responses):
    """Mock response from Titelive API for music category."""
    return sample_responses["sample_music_response"]


@pytest.fixture()
def mock_empty_titelive_response():
    """Mock empty response from Titelive API."""
    return {"result": []}


@pytest.fixture()
def mock_token_response():
    """Mock token response from Titelive login endpoint."""
    return {"token": "mock_bearer_token_123"}


@pytest.fixture()
def sample_raw_dataframe():
    """Sample raw DataFrame as output from extract script."""
    data = [
        {
            "id": "book_123",
            "titre": "Test Book Title",
            "auteurs_multi": [{"nom": "Test Author", "prenom": "John"}],
            "article": {
                "art1": {
                    "ean": "9781234567890",
                    "prix": 15.99,
                    "datemodification": "01/01/2024",
                    "taux_tva": 5.5,
                    "image": 1,
                    "iad": 0,
                    "typeproduit": 1,
                }
            },
        },
        {
            "id": "book_456",
            "titre": "Another Test Book",
            "auteurs_multi": [{"nom": "Another Author", "prenom": "Jane"}],
            "article": {
                "art2": {
                    "ean": "9781234567891",
                    "prix": 12.50,
                    "datemodification": "02/01/2024",
                    "taux_tva": 5.5,
                    "image": 0,
                    "iad": 1,
                    "typeproduit": 2,
                }
            },
        },
    ]

    return pd.DataFrame(
        {
            "id": [item["id"] for item in data],
            "data": [json.dumps(item, ensure_ascii=False) for item in data],
        }
    ).set_index("id")


@pytest.fixture()
def sample_parsed_dataframe():
    """Sample parsed DataFrame as output from parse script."""
    return pd.DataFrame(
        {
            "titre": ["Test Book Title", "Another Test Book"],
            "auteurs_multi": [
                '{"nom": "Test Author", "prenom": "John"}',
                '{"nom": "Another Author", "prenom": "Jane"}',
            ],
            "article_ean": ["9781234567890", "9781234567891"],
            "article_prix": [15.99, 12.50],
            "article_datemodification": ["01/01/2024", "02/01/2024"],
            "article_taux_tva": [5.5, 5.5],
            "article_image": [1, 0],
            "article_iad": [0, 1],
            "article_typeproduit": [1, 2],
        }
    )


@pytest.fixture()
def mock_secret_manager():
    """Mock for Google Cloud Secret Manager."""
    mock = Mock()
    mock.access_secret_version.return_value.payload.data.decode.return_value = (
        "mock_secret_value"
    )
    return mock


@pytest.fixture()
def sample_date():
    """Sample date for testing."""
    return datetime(2024, 1, 1)


@pytest.fixture()
def mock_requests_response():
    """Mock requests response object."""
    mock = Mock()
    mock.status_code = 200
    mock.encoding = "utf-8"
    mock.raise_for_status.return_value = None
    return mock
