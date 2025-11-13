"""Pytest configuration and fixtures for all tests."""

import pytest

from src.utils.mlflow import optional_mlflow_logging


@pytest.fixture(autouse=True)
def _disable_mlflow_logging():
    """Automatically disable MLflow logging for all tests."""
    with optional_mlflow_logging(enabled=False):
        yield
