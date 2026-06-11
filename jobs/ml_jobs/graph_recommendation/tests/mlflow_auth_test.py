"""Tests for MLflowAuthManager using signed JWTs."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.utils.mlflow import MLflowAuthManager


@pytest.fixture()
def _clean_mlflow_env():
    """Backup and clean MLflow tracking environment variables."""
    old_token = os.environ.get("MLFLOW_TRACKING_TOKEN")
    old_uri = os.environ.get("MLFLOW_TRACKING_URI")

    if "MLFLOW_TRACKING_TOKEN" in os.environ:
        del os.environ["MLFLOW_TRACKING_TOKEN"]
    if "MLFLOW_TRACKING_URI" in os.environ:
        del os.environ["MLFLOW_TRACKING_URI"]

    yield

    if old_token is not None:
        os.environ["MLFLOW_TRACKING_TOKEN"] = old_token
    else:
        os.environ.pop("MLFLOW_TRACKING_TOKEN", None)

    if old_uri is not None:
        os.environ["MLFLOW_TRACKING_URI"] = old_uri
    else:
        os.environ.pop("MLFLOW_TRACKING_URI", None)


@pytest.mark.usefixtures("_clean_mlflow_env")
@patch("src.utils.mlflow.google.auth.default")
@patch("src.utils.mlflow.iam_credentials_v1.IAMCredentialsClient")
def test_mlflow_auth_manager_authenticate(mock_iam_client_cls, mock_auth_default):
    # Mock google auth credentials
    mock_creds = MagicMock()
    mock_auth_default.return_value = (mock_creds, "mock-project-id")

    # Mock IAM Credentials Client response
    mock_client = MagicMock()
    mock_client.service_account_path.return_value = (
        "projects/-/serviceAccounts/mock-sa@mock-project.iam.gserviceaccount.com"
    )

    mock_response = MagicMock()
    mock_response.signed_jwt = "mocked-signed-jwt-token"
    mock_client.sign_jwt.return_value = mock_response

    mock_iam_client_cls.return_value = mock_client

    # Initialize Auth Manager
    auth_manager = MLflowAuthManager(
        mlflow_uri="https://mlflow.test.team/",
        sa_account="algo-training-dev@passculture-data-ehp.iam.gserviceaccount.com",
        token_refresh_interval=300,
    )

    # Perform authentication
    auth_manager.authenticate()

    # Verify token environment variable and MLflow tracking URI are correctly set
    assert auth_manager.token == "mocked-signed-jwt-token"
    assert os.environ.get("MLFLOW_TRACKING_TOKEN") == "mocked-signed-jwt-token"

    # Verify mock calls
    mock_auth_default.assert_called_once()
    mock_client.sign_jwt.assert_called_once()


@pytest.mark.usefixtures("_clean_mlflow_env")
@patch("src.utils.mlflow.google.auth.default")
@patch("src.utils.mlflow.iam_credentials_v1.IAMCredentialsClient")
def test_mlflow_auth_manager_refresh_token(mock_iam_client_cls, mock_auth_default):
    mock_creds = MagicMock()
    mock_auth_default.return_value = (mock_creds, "mock-project-id")

    mock_client = MagicMock()
    mock_response_1 = MagicMock()
    mock_response_1.signed_jwt = "token-1"
    mock_response_2 = MagicMock()
    mock_response_2.signed_jwt = "token-2"

    mock_client.sign_jwt.side_effect = [mock_response_1, mock_response_2]
    mock_iam_client_cls.return_value = mock_client

    # Initialize with short refresh interval
    auth_manager = MLflowAuthManager(
        mlflow_uri="https://mlflow.test.team/",
        sa_account="algo-training-dev@passculture-data-ehp.iam.gserviceaccount.com",
        token_refresh_interval=2,
    )

    # Authenticate (calls sign_jwt first time)
    auth_manager.authenticate()
    assert auth_manager.token == "token-1"
    assert mock_client.sign_jwt.call_count == 1

    # Try refreshing immediately (should not refresh because interval hasn't elapsed)
    auth_manager.refresh_token()
    assert auth_manager.token == "token-1"
    assert mock_client.sign_jwt.call_count == 1

    # Mock time passing beyond refresh interval
    with patch("src.utils.mlflow.time.time") as mock_time:
        # Initial refresh timestamp was set at time.time()
        # Let's say authenticate ran at t=100
        # Now current time is t=105 (interval is 2)
        mock_time.return_value = auth_manager._last_refresh_ts + 5

        auth_manager.refresh_token()
        assert auth_manager.token == "token-2"
        assert mock_client.sign_jwt.call_count == 2
