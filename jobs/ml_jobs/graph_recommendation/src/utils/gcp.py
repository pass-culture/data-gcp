import os
import tempfile
from pathlib import Path

import pandas as pd
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager, storage
from google.oauth2 import service_account
from loguru import logger

from src.constants import ENV_SHORT_NAME, GCP_PROJECT_ID

BUCKET_PREFIX = "gs://"
SA_ACCOUNT = f"algo-training-{ENV_SHORT_NAME}"


def is_bucket_path(path: str) -> bool:
    """Check if a given path is a cloud storage bucket path."""
    return path.startswith(BUCKET_PREFIX)


def get_credentials():
    """
    Get GCP credentials with fallback strategy.

    Priority:
    1. Service account from GOOGLE_APPLICATION_CREDENTIALS env var
    2. Default credentials (gcloud auth application-default login)

    Returns:
        google.auth.credentials.Credentials: The credentials object

    Raises:
        RuntimeError: If no credentials are available
    """
    sa_key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if sa_key_path:
        try:
            logger.info(f"Using service account from: {sa_key_path}")
            credentials = service_account.Credentials.from_service_account_file(
                sa_key_path
            )
            return credentials
        except Exception as e:
            logger.warning(
                f"Failed to load service account: {e}. "
                "Falling back to default credentials."
            )

    try:
        logger.info("Using default credentials (gcloud)")
        credentials, _project = default()
        return credentials
    except DefaultCredentialsError as e:
        raise RuntimeError(
            "No credentials found. Either:\n"
            "1. Set GOOGLE_APPLICATION_CREDENTIALS to your service account key path\n"
            "2. Run: gcloud auth application-default login"
        ) from e


def get_secret(secret_id: str):
    """Get secret from Secret Manager using available credentials."""
    credentials = get_credentials()
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def save_query_score_details_to_gcs(
    results_df: pd.DataFrame,
    score_details_bucket_folder: str,
) -> None:
    """
    Save detailed query-scores DataFrame to a Parquet file in GCS,
    optionally partitioned by columns (Hive-style).

    Args:
        results_df: DataFrame to save.
        score_details_bucket_folder: GCS folder path, e.g., "gs://my-bucket/folder".
        partition_cols: List of columns to partition by.
    """

    client = storage.Client()
    # Parse bucket and prefix
    if not score_details_bucket_folder.startswith("gs://"):
        raise ValueError("score_details_bucket_folder must start with 'gs://'")
    bucket_name = score_details_bucket_folder.split("/", 1)
    _bucket = client.bucket(bucket_name)
    # Save the full DataFrame as Parquet and publish to GCS
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "query_score_details.parquet"
        results_df.to_parquet(local_path, index=False)

    logger.info(f"Pairwise scores saved to: {score_details_bucket_folder}")
