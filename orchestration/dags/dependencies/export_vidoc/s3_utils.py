import logging
from typing import Any, Dict

import boto3
from botocore.client import Config
from google.cloud import storage

logger = logging.getLogger(__name__)

_DELETE_CHUNK = 1000


def init_s3_client(s3_config: Dict[str, Any]):
    """Initialise a boto3 client targeting an S3-compatible endpoint (OVH)."""
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


def upload_gcs_prefix_to_s3(
    gcs_bucket: str,
    gcs_prefix: str,
    s3_client,
    s3_bucket: str,
    s3_prefix: str,
    key_prefix: str = "",
) -> set:
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(gcs_bucket)
    blobs = [
        blob
        for blob in bucket.list_blobs(prefix=f"{gcs_prefix}/")
        if not blob.name.endswith("/")
    ]
    if not blobs:
        raise ValueError(f"No objects found at gs://{gcs_bucket}/{gcs_prefix}/")

    uploaded_keys: set = set()
    for blob in blobs:
        filename = blob.name.rsplit("/", 1)[-1]
        name = f"{key_prefix}-{filename}" if key_prefix else filename
        key = f"{s3_prefix}/{name}"
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=blob.download_as_bytes(),
        )
        uploaded_keys.add(key)
        logger.info(
            "Uploaded gs://%s/%s to s3://%s/%s",
            gcs_bucket,
            blob.name,
            s3_bucket,
            key,
        )
    return uploaded_keys


def prune_stale_s3_objects(s3_client, bucket: str, prefix: str, keep_keys: set) -> int:
    """Delete every key under `prefix` not present in `keep_keys`.

    Run AFTER a successful upload so the freshly written keys (which must be in
    `keep_keys`) are never targets of the delete batch. This avoids the
    delete-then-put race that wiping the prefix up-front exposed on
    eventually-consistent S3-compatible endpoints.
    """
    stale = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"] not in keep_keys:
                stale.append({"Key": obj["Key"]})

    deleted = 0
    for i in range(0, len(stale), _DELETE_CHUNK):
        batch = stale[i : i + _DELETE_CHUNK]
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)
    logger.info("Pruned %d stale objects from s3://%s/%s", deleted, bucket, prefix)
    return deleted
