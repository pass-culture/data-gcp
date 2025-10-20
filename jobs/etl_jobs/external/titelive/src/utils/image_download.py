"""Async utilities for downloading images from URLs and uploading to GCS."""

import asyncio

import aiohttp
from google.cloud import storage

from src.utils.logging import get_logger

logger = get_logger(__name__)


async def download_and_upload_image(
    session: aiohttp.ClientSession,
    storage_client: storage.Client,
    image_url: str,
    gcs_path: str,
    semaphore: asyncio.Semaphore,
    max_retries: int = 3,
    timeout_seconds: int = 30,
) -> tuple[bool, str, str]:
    """
    Download image from URL and upload to GCS asynchronously.

    Args:
        session: aiohttp ClientSession for connection pooling
        storage_client: GCS client instance
        image_url: URL of the image to download
        gcs_path: Full GCS path (gs://bucket/path/to/image.jpg)
        semaphore: asyncio.Semaphore to limit concurrent downloads
        max_retries: Maximum number of retry attempts
        timeout_seconds: Request timeout in seconds

    Returns:
        tuple: (success: bool, image_url: str, message: str)
    """
    async with semaphore:
        logger.debug(f"Starting download: {image_url}")

        for attempt in range(max_retries):
            try:
                # Download image with timeout
                logger.debug(
                    f"Attempt {attempt + 1}/{max_retries}: Downloading {image_url}"
                )
                timeout = aiohttp.ClientTimeout(total=timeout_seconds)
                async with session.get(
                    image_url,
                    timeout=timeout,
                    headers={"User-Agent": "TiteliveETL/2.0"},
                ) as response:
                    response.raise_for_status()

                    # Read image content
                    image_content = await response.read()
                    content_type = response.headers.get("Content-Type", "image/jpeg")
                    logger.debug(
                        f"Downloaded {len(image_content)} bytes "
                        f"(Content-Type: {content_type}) from {image_url}"
                    )

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

                # Upload to GCS (sync operation in thread pool)
                logger.debug(f"Uploading to GCS: {gcs_path}")
                await asyncio.to_thread(
                    _upload_to_gcs_sync,
                    storage_client,
                    bucket_name,
                    blob_path,
                    image_content,
                    content_type,
                )

                logger.debug(f"Successfully uploaded: {gcs_path}")
                return True, image_url, "Success"

            except aiohttp.ClientError as e:
                error_msg = f"HTTP error: {e}"
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.debug(
                        f"Retry {attempt + 1}/{max_retries} for {image_url} "
                        f"after {wait_time}s: {error_msg}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.warning(
                    f"Failed after {max_retries} retries: {image_url} - {error_msg}"
                )
                return False, image_url, error_msg

            except TimeoutError:
                error_msg = f"Timeout after {timeout_seconds}s"
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    logger.debug(
                        f"Retry {attempt + 1}/{max_retries} for {image_url} "
                        f"after {wait_time}s: {error_msg}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                logger.warning(
                    f"Failed after {max_retries} retries: {image_url} - {error_msg}"
                )
                return False, image_url, error_msg

            except Exception as e:
                error_msg = f"Unexpected error: {e}"
                logger.error(f"Unexpected error for {image_url}: {e}")
                return False, image_url, error_msg

    error_msg = "Max retries exceeded"
    logger.warning(f"Failed: {image_url} - {error_msg}")
    return False, image_url, error_msg


def _upload_to_gcs_sync(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    image_content: bytes,
    content_type: str,
) -> None:
    """
    Synchronous GCS upload (called via asyncio.to_thread).

    Args:
        storage_client: GCS client instance
        bucket_name: GCS bucket name
        blob_path: Path within the bucket
        image_content: Image bytes to upload
        content_type: Content-Type header for the blob
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(image_content, content_type=content_type)


async def create_session(
    connector_limit: int = 100, connector_limit_per_host: int = 30
) -> aiohttp.ClientSession:
    """
    Create aiohttp ClientSession with connection pooling.

    Args:
        connector_limit: Total connection pool size
        connector_limit_per_host: Max connections per host

    Returns:
        Configured aiohttp ClientSession
    """
    connector = aiohttp.TCPConnector(
        limit=connector_limit,
        limit_per_host=connector_limit_per_host,
        ttl_dns_cache=300,  # Cache DNS for 5 minutes
    )
    return aiohttp.ClientSession(connector=connector)


async def batch_download_and_upload(
    image_urls: list[str],
    gcs_paths: list[str],
    storage_client: storage.Client,
    max_concurrent: int = 100,
    max_retries: int = 3,
    timeout_seconds: int = 30,
) -> list[tuple[bool, str, str]]:
    """
    Download and upload multiple images concurrently.

    Args:
        image_urls: List of image URLs to download
        gcs_paths: List of corresponding GCS destination paths
        storage_client: GCS client instance
        max_concurrent: Maximum concurrent downloads
        max_retries: Maximum retry attempts per image
        timeout_seconds: Request timeout in seconds

    Returns:
        List of (success, url, message) tuples
    """
    if len(image_urls) != len(gcs_paths):
        raise ValueError("image_urls and gcs_paths must have the same length")

    total_images = len(image_urls)
    logger.info(
        f"Starting async download of {total_images} "
        "images (max_concurrent={max_concurrent})"
    )

    semaphore = asyncio.Semaphore(max_concurrent)
    session = await create_session()

    try:
        tasks = [
            download_and_upload_image(
                session=session,
                storage_client=storage_client,
                image_url=url,
                gcs_path=gcs_path,
                semaphore=semaphore,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
            )
            for url, gcs_path in zip(image_urls, gcs_paths, strict=False)
        ]

        logger.info(f"Created {len(tasks)} async tasks, awaiting completion...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions from gather
        processed_results = []
        success_count = 0
        failed_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(
                    (False, image_urls[i], f"Task failed: {result}")
                )
                failed_count += 1
            else:
                processed_results.append(result)
                if result[0]:  # result[0] is success boolean
                    success_count += 1
                else:
                    failed_count += 1

        logger.info(
            f"Async batch complete: {total_images} images processed, "
            f"{success_count} success ({success_count/total_images*100:.1f}%), "
            f"{failed_count} failed ({failed_count/total_images*100:.1f}%)"
        )

        return processed_results

    finally:
        await session.close()
        logger.debug("Closed aiohttp session")
