"""ETL job specific fixtures for unit testing."""

import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def setup_etl_test_environment():
    """Set up ETL-specific test environment."""
    # Mock common environment variables for ETL jobs
    import os

    etl_env = {
        "ETL_ENV": "test",
        "BQ_PROJECT": "test-project",
        "BQ_DATASET": "test_dataset",
        "GCS_BUCKET": "test-etl-bucket",
        "API_TIMEOUT": "30",
        "BATCH_SIZE": "1000",
        "ENV_SHORT_NAME": "dev",
    }

    with patch.dict(os.environ, etl_env, clear=False):
        yield
