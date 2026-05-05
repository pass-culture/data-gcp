import os
from unittest.mock import MagicMock, patch

import pytest

# Set env vars BEFORE any application imports (core.utils reads them at import time)
os.environ.setdefault("ENV_SHORT_NAME", "dev")
os.environ.setdefault("PROJECT_NAME", "test-project")

# Patch secret manager before core.utils is imported
_mock_secret = patch(
    "google.cloud.secretmanager.SecretManagerServiceClient",
    return_value=MagicMock(),
)
_mock_secret.start()


@pytest.fixture
def metabase():
    """A MagicMock standing in for MetabaseAPI."""
    return MagicMock()
