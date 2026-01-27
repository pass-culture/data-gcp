"""Utilities for downloading images from URLs and uploading
to GCS using ThreadPoolExecutor."""

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.cloud import storage
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry
from workflows.titelive.logging_utils import get_logger

logger = get_logger(__name__)


def calculate_url_uuid(url: str) -> str:
    """
    Calculate deterministic UUID5 hash for an image URL.

    Uses UUID5 with NAMESPACE_URL to generate a deterministic UUID
    based on the URL. Same URL always produces same UUID.

    Args:
        url: Image URL to hash

    Returns:
        UUID5 as string (without hyphens for GCS filename compatibility)
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, url))


def _get_session(
    pool_connections: int = 10,
    pool_maxsize: int = 20,
    timeout: int = 60,
) -> requests.Session:
    """
    Create a requests session with connection pooling and retries.

    Args:
        pool_connections: Number of connection pools to cache
        pool_maxsize: Maximum number of connections to save in the pool
        timeout: Request timeout in seconds

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=10,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    # Configure adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )

    session.mount("https://", adapter)

    return session


def _download_and_upload_to_gcs(
    storage_client: storage.Client,
    session: requests.Session,
    image_url: str,
    gcs_path: str,
    timeout: int = 60,
) -> tuple[bool, str, str]:
    """
    Download image from URL and upload to GCS.

    Args:
        storage_client: GCS client instance
        session: requests.Session for connection pooling
        image_url: URL of the image to download
        gcs_path: Full GCS path (gs://bucket/path/to/image.jpg)
        timeout: Request timeout in seconds

    Returns:
        tuple: (success: bool, image_url: str, message: str)
    """
    try:
        # Validate GCS path
        if not gcs_path.startswith("gs://"):
            error_msg = f"Invalid GCS path format: {gcs_path}"
            logger.error(f"{error_msg} for {image_url}")
            return False, image_url, error_msg

        # Parse GCS bucket and blob path
        parts = gcs_path[len("gs://") :].split("/", 1)
        if len(parts) != 2:
            error_msg = f"Invalid GCS path structure: {gcs_path}"
            logger.error(f"{error_msg} for {image_url}")
            return False, image_url, error_msg

        bucket_name, blob_path = parts

        # Download image
        response = session.get(
            image_url,
            timeout=timeout,
            headers={"User-Agent": "TiteliveETL/2.0"},
        )
        response.raise_for_status()

        # Upload to GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        content_type = response.headers.get("Content-Type", "image/jpeg")
        blob.upload_from_string(response.content, content_type=content_type)

        logger.debug(f"Successfully uploaded: {image_url} -> {gcs_path}")
        return True, image_url, "Success"

    except requests.exceptions.Timeout:
        error_msg = "Timeout error"
        logger.warning(f"Failed: {image_url} - {error_msg}")
        return False, image_url, error_msg

    except requests.exceptions.ConnectionError as e:
        error_msg = f"Connection error: {e}"
        logger.warning(f"Failed: {image_url} - {error_msg}")
        return False, image_url, error_msg

    except requests.exceptions.RequestException as e:
        error_msg = f"HTTP error: {e}"
        logger.warning(f"Failed: {image_url} - {error_msg}")
        return False, image_url, error_msg

    except Exception as e:
        error_msg = f"GCS upload or other error: {e}"
        logger.error(f"Failed: {image_url} - {error_msg}")
        return False, image_url, error_msg


def batch_download_and_upload(
    storage_client: storage.Client,
    session: requests.Session,
    image_urls: list[str],
    gcs_paths: list[str],
    max_workers: int = 50,
    timeout: int = 60,
) -> list[tuple[bool, str, str]]:
    """
    Download and upload multiple images concurrently using ThreadPoolExecutor.

    Args:
        storage_client: GCS client instance
        session: requests.Session for connection pooling
        image_urls: List of image URLs to download
        gcs_paths: List of corresponding GCS destination paths
        max_workers: Maximum number of concurrent workers
        timeout: HTTP request timeout in seconds

    Returns:
        List of (success, url, message) tuples
    """
    if len(image_urls) != len(gcs_paths):
        raise ValueError("image_urls and gcs_paths must have the same length")

    total_images = len(image_urls)
    logger.info(
        f"Starting threaded download of {total_images} images "
        f"(max_workers={max_workers})"
    )

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                _download_and_upload_to_gcs,
                storage_client,
                session,
                url,
                gcs_path,
                timeout,
            ): (url, gcs_path)
            for url, gcs_path in zip(image_urls, gcs_paths, strict=False)
        }

        # Collect results with progress bar
        for future in tqdm(
            as_completed(futures),
            total=total_images,
            desc="Downloading images",
            unit="img",
        ):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                url, _ = futures[future]
                logger.error(f"Task exception for {url}: {e}")
                results.append((False, url, f"Task failed: {e}"))

    # Count results
    success_count = sum(1 for r in results if r[0])
    failed_count = total_images - success_count

    logger.info(
        f"Threaded batch complete: {total_images} images processed, "
        f"{success_count} success ({success_count/total_images*100:.1f}%), "
        f"{failed_count} failed ({failed_count/total_images*100:.1f}%)"
    )

    return results
