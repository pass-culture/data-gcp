import os
import json
import boto3
from typing import Dict, Any
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from botocore.client import Config
from loguru import logger
import sys
import multiprocessing

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)

FILE_EXTENSION = ".parquet"
PROJECT_NAME = os.environ.get("GCP_PROJECT_ID")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
PREFIX_S3_SECRET = "dbt_export_s3_config"
ENCRYPTED_FOLDER = "tmp_encrypted_folder"
DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_WORKERS = multiprocessing.cpu_count()


def init_s3_client(s3_config: Dict[str, Any]) -> boto3.client:
    """
    Initialize and return an S3 client using the provided configuration.

    Args:
        s3_config (Dict[str, Any]): The configuration dictionary for S3.

    Returns:
        boto3.client: An S3 client instance.
    """
    # Store configuration state with a session
    _ = boto3.session.Session()
    return boto3.client(
        "s3",
        aws_access_key_id=s3_config["target_access_key"],
        aws_secret_access_key=s3_config["target_secret_key"],
        endpoint_url=s3_config["target_endpoint_url"],
        region_name=s3_config["target_s3_region"],
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def load_target_bucket_config(partner_name: str) -> Dict[str, Any]:
    """
    Load the target bucket configuration from the secret manager.

    Args:
        partner_name (str): The name of the partner.

    Returns:
        Dict[str, Any]: The target bucket configuration.
    """
    secret_id = f"{PREFIX_S3_SECRET}_{partner_name}"

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_NAME}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        access_secret_data = response.payload.data.decode("UTF-8")
        return json.loads(access_secret_data)
    except DefaultCredentialsError:
        return {}
