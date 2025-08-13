"""Contentful-specific test configuration and fixtures."""

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# Create a mock utils module that will be imported instead of the real one
class MockUtils:
    """Mock utils module for contentful tests."""

    def __init__(self):
        # Mock the secret values that would normally be fetched
        self.PREVIEW_TOKEN = "test-preview-token"
        self.TOKEN = "test-token"
        self.SPACE_ID = "test-space-id"
        self.ENTRIES_DTYPE = {
            "title": str,
            "contentful_tags_id": str,
            "contentful_tags_name": str,
        }

    def access_secret_data(
        self, project_id, secret_id, version_id="latest", default=None
    ):
        """Mock implementation of access_secret_data for testing."""
        secret_values = {
            "contentful-preview-token": "test-preview-token",
            "contentful-token": "test-token",
            "contentful-space-id": "test-space-id",
        }
        return secret_values.get(secret_id, f"test-{secret_id}")


# Install the mock module before any imports happen
mock_utils = MockUtils()
sys.modules["jobs.etl_jobs.external.contentful.utils"] = mock_utils
sys.modules["utils"] = mock_utils  # Also handle relative imports


@pytest.fixture
def mock_contentful_client():
    """Mock contentful client with proper method signatures."""
    with patch("contentful.Client") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock the entries method to return a mock entries response
        mock_entries_response = MagicMock()
        mock_entries_response.total = 5  # Default total entries
        mock_entries_response.__iter__ = lambda x: iter([])
        mock_entries_response.__len__ = lambda x: 0
        mock_client.entries.return_value = mock_entries_response

        # Mock environment_url method (it's a method, not a property)
        mock_client.environment_url = MagicMock(
            return_value="https://cdn.contentful.com/spaces/test-space-id/environments/test"
        )

        # Mock _http_get method for tag name retrieval
        mock_http_response = MagicMock()
        mock_http_response.json.return_value = {"name": "test-tag-name"}
        mock_client._http_get.return_value = mock_http_response

        yield mock_client


@pytest.fixture
def mock_contentful_entry():
    """Create a mock contentful entry with proper structure."""
    mock_entry = MagicMock()
    mock_entry.id = "test-entry-123"
    mock_entry.title = "Test Entry Title"  # Add title attribute

    # Create mock objects for nested sys fields that have .id attributes
    mock_space = MagicMock()
    mock_space.id = "test-space-id"

    mock_content_type = MagicMock()
    mock_content_type.id = "homepageNatif"

    mock_environment = MagicMock()
    mock_environment.id = "test"

    mock_entry.sys = {
        "id": "test-entry-123",
        "content_type": mock_content_type,
        "created_at": datetime(2023, 1, 1),
        "updated_at": datetime(2023, 1, 2),
        "space": mock_space,
        "environment": mock_environment,
        "revision": 1,
        "type": "Entry",
        "locale": "en-US",
    }

    # Mock the fields method
    mock_entry.fields.return_value = {
        "title": "Test Entry Title",
        "modules": [],
        "description": "Test description",
    }

    # Mock metadata with tags
    mock_tag = MagicMock()
    mock_tag.id = "test-tag-123"
    mock_entry._metadata = {"tags": [mock_tag]}

    return mock_entry


@pytest.fixture
def sample_module_details():
    """Provide sample module details for testing."""
    return {
        "name": "homepageNatif",
        "additional_fields": ["title", "modules"],
        "children": [],
    }


@pytest.fixture
def sample_algolia_module_details():
    """Provide sample algolia module details for testing."""
    return {
        "name": "algolia",
        "additional_fields": [
            "title",
            "cover",
            "display_parameters",
            "algolia_parameters",
        ],
        "children": [],
    }
