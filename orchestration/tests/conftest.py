"""Minimal conftest.py for testing delayed_waiting_operator."""

import pytest
import datetime
import sys
from pathlib import Path
from unittest.mock import Mock

# Add dags directory to Python path so all test files can import from common
DAGS_PATH = Path(__file__).parent.parent / "dags"
sys.path.insert(0, str(DAGS_PATH))

from airflow import DAG


@pytest.fixture
def test_dag():
    """Create a standard test DAG that all tests can use."""
    return DAG(
        "test_dag",
        start_date=datetime.datetime(2024, 1, 1),
        schedule="@daily",  # Changed from schedule_interval
        catchup=False,
    )


@pytest.fixture
def manual_dag_run():
    """Create a mock manually triggered DAG run."""
    dag_run = Mock()
    dag_run.run_id = "manual__2024-01-15T12:00:00"
    return dag_run


@pytest.fixture
def scheduled_dag_run():
    """Create a mock scheduled DAG run."""
    dag_run = Mock()
    dag_run.run_id = "scheduled__2024-01-15T12:00:00"
    return dag_run
